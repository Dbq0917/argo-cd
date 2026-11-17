package ratelimiter

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func withClientRateLimiter(t *testing.T, enabled bool) {
	t.Helper()
	previous := ClientRateLimiterEnabled()
	SetClientRateLimiterEnabled(enabled)
	t.Cleanup(func() {
		SetClientRateLimiterEnabled(previous)
	})
}

func TestDynamicClientRateLimiter_EnabledThrottles(t *testing.T) {
	withClientRateLimiter(t, true)

	limiter := NewDynamicClientRateLimiter(10, 2)
	defer limiter.Stop()

	// Burst of 2 is available immediately, the third call must not be.
	assert.True(t, limiter.TryAccept())
	assert.True(t, limiter.TryAccept())
	assert.False(t, limiter.TryAccept())
	assert.InDelta(t, 10, limiter.QPS(), 0.0001)

	// Wait must block until a token is refilled (~100ms at 10 QPS).
	start := time.Now()
	require.NoError(t, limiter.Wait(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestDynamicClientRateLimiter_DisabledIsUnlimited(t *testing.T) {
	withClientRateLimiter(t, false)

	limiter := NewDynamicClientRateLimiter(1, 1)
	defer limiter.Stop()

	for range 1000 {
		assert.True(t, limiter.TryAccept())
	}
	start := time.Now()
	for range 1000 {
		require.NoError(t, limiter.Wait(context.Background()))
	}
	assert.Less(t, time.Since(start), time.Second)
	assert.True(t, math.IsInf(float64(limiter.QPS()), 1))
}

func TestDynamicClientRateLimiter_RuntimeToggle(t *testing.T) {
	withClientRateLimiter(t, true)

	limiter := NewDynamicClientRateLimiter(1, 1)
	defer limiter.Stop()

	// Drain the single burst token.
	assert.True(t, limiter.TryAccept())
	assert.False(t, limiter.TryAccept())

	// Disabling at runtime must take effect immediately on the same instance.
	SetClientRateLimiterEnabled(false)
	assert.True(t, limiter.TryAccept())

	// Re-enabling restores the configured token bucket behavior.
	SetClientRateLimiterEnabled(true)
	assert.False(t, limiter.TryAccept())
}

// TestDynamicClientRateLimiter_RetryAfterHonored verifies that disabling the
// client-side rate limiter does not disable client-go retries: when the server
// (platform-side rate limiter) rejects a request with HTTP 429 and a
// Retry-After hint, the client backs off for at least the suggested duration
// instead of retrying immediately.
func TestDynamicClientRateLimiter_RetryAfterHonored(t *testing.T) {
	withClientRateLimiter(t, false)

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &rest.Config{
		Host:        server.URL,
		RateLimiter: NewDynamicClientRateLimiter(1, 1),
	}
	config.GroupVersion = &schema.GroupVersion{Version: "v1"}
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	restClient, err := rest.RESTClientFor(config)
	require.NoError(t, err)

	start := time.Now()
	err = restClient.Get().AbsPath("/api/v1/namespaces").Do(t.Context()).Error()
	require.NoError(t, err)

	assert.Equal(t, int32(2), attempts.Load())
	assert.GreaterOrEqual(t, time.Since(start), time.Second, "client must honor the server-provided Retry-After delay")
}

// TestDynamicClientRateLimiter_PlatformSidePacing is the acceptance scenario for
// disabling the client-side rate limiter:
//  1. With the client-side limiter disabled, a burst of requests against a
//     platform-side rate limiter (HTTP 429 + Retry-After) is paced by the
//     platform: requests are retried after the suggested backoff and all
//     eventually succeed, instead of being stuck behind a client-side bucket.
//  2. With the client-side limiter re-enabled, throughput is capped by the
//     configured client-side QPS again, exactly as before the change.
func TestDynamicClientRateLimiter_PlatformSidePacing(t *testing.T) {
	previous := ClientRateLimiterEnabled()
	t.Cleanup(func() {
		SetClientRateLimiterEnabled(previous)
	})

	// platformQuota requests per second are allowed; anything beyond that is
	// rejected with 429 and a Retry-After hint, like a platform-side limiter.
	const platformQuota = 5
	var mu sync.Mutex
	windowStart := time.Now()
	served := 0
	var rejected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(windowStart) >= time.Second {
			windowStart = time.Now()
			served = 0
		}
		served++
		if served > platformQuota {
			rejected.Add(1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	newClient := func(qps float32, burst int) *rest.RESTClient {
		config := &rest.Config{
			Host:        server.URL,
			RateLimiter: NewDynamicClientRateLimiter(qps, burst),
		}
		config.GroupVersion = &schema.GroupVersion{Version: "v1"}
		config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
		restClient, err := rest.RESTClientFor(config)
		require.NoError(t, err)
		return restClient
	}

	doRequests := func(restClient *rest.RESTClient, count int) time.Duration {
		start := time.Now()
		for range count {
			require.NoError(t, restClient.Get().AbsPath("/api/v1/namespaces").Do(t.Context()).Error())
		}
		return time.Since(start)
	}

	// Phase 1: client-side limiter disabled. 12 requests against a platform
	// quota of 5/s must be paced by the platform (>= 2s) and must all succeed
	// via Retry-After backoff. Without the toggle, a client-side bucket at the
	// default 50 QPS would instead serialize them client-side.
	SetClientRateLimiterEnabled(false)
	elapsed := doRequests(newClient(50, 50), 12)
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "request pace must be dictated by the platform-side limiter")
	assert.Positive(t, rejected.Load(), "platform-side rejections must have occurred and been retried")

	// Phase 2: client-side limiter re-enabled at 5 QPS (burst 1). The platform
	// would allow everything immediately, but 6 requests must take ~1s because
	// the client-side token bucket throttles exactly as before the change.
	SetClientRateLimiterEnabled(true)
	elapsed = doRequests(newClient(5, 1), 6)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "enabled client-side limiter must throttle as before")
}

// BenchmarkClientRateLimiter measures request throughput with the client-side
// rate limiter enabled (50 QPS, the default) versus disabled, demonstrating
// that with the limiter off, request pacing is no longer constrained
// client-side. Run with: go test -bench=ClientRateLimiter -benchtime=50x ./pkg/ratelimiter
func BenchmarkClientRateLimiter(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	for _, enabled := range []bool{true, false} {
		b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
			SetClientRateLimiterEnabled(enabled)
			defer SetClientRateLimiterEnabled(true)

			config := &rest.Config{
				Host:        server.URL,
				RateLimiter: NewDynamicClientRateLimiter(50, 50),
			}
			config.GroupVersion = &schema.GroupVersion{Version: "v1"}
			config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
			restClient, err := rest.RESTClientFor(config)
			require.NoError(b, err)

			b.ResetTimer()
			for b.Loop() {
				_ = restClient.Get().AbsPath("/api/v1/namespaces").Do(b.Context()).Error()
			}
		})
	}
}
