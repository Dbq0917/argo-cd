package ratelimiter

import (
	"context"
	"math"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/util/flowcontrol"
)

// clientRateLimiterEnabled controls whether the client-side Kubernetes API rate
// limiter is enforced. It is read by every DynamicClientRateLimiter instance on
// each call, so toggling it takes effect immediately for all REST clients in
// the process without a restart.
var clientRateLimiterEnabled atomic.Bool

func init() {
	clientRateLimiterEnabled.Store(true)
}

// SetClientRateLimiterEnabled enables or disables the client-side Kubernetes API
// rate limiter process-wide. Every state change is audit logged.
func SetClientRateLimiterEnabled(enabled bool) {
	previous := clientRateLimiterEnabled.Swap(enabled)
	if previous != enabled {
		log.Infof("AUDIT: client-side Kubernetes API rate limiter toggled: enabled=%t (previous=%t)", enabled, previous)
	}
}

// ClientRateLimiterEnabled reports whether the client-side Kubernetes API rate
// limiter is currently enforced.
func ClientRateLimiterEnabled() bool {
	return clientRateLimiterEnabled.Load()
}

// DynamicClientRateLimiter is a flowcontrol.RateLimiter that delegates to the
// standard client-go token bucket rate limiter while enabled, and imposes no
// client-side throttling while disabled. The enabled/disabled state is
// process-wide and can be flipped at runtime via SetClientRateLimiterEnabled.
//
// Disabling the limiter only removes client-side request throttling. It does
// not affect client-go retries (which honor the server-provided Retry-After
// header on HTTP 429 responses) or any server-side timeout behavior.
type DynamicClientRateLimiter struct {
	inner flowcontrol.RateLimiter
	qps   float32
}

var _ flowcontrol.RateLimiter = &DynamicClientRateLimiter{}

// NewDynamicClientRateLimiter returns a rate limiter that behaves exactly like
// flowcontrol.NewTokenBucketRateLimiter(qps, burst) while the client-side rate
// limiter is enabled, and is unlimited while it is disabled.
func NewDynamicClientRateLimiter(qps float32, burst int) flowcontrol.RateLimiter {
	return &DynamicClientRateLimiter{
		inner: flowcontrol.NewTokenBucketRateLimiter(qps, burst),
		qps:   qps,
	}
}

func (d *DynamicClientRateLimiter) TryAccept() bool {
	if !clientRateLimiterEnabled.Load() {
		return true
	}
	return d.inner.TryAccept()
}

func (d *DynamicClientRateLimiter) Accept() {
	if !clientRateLimiterEnabled.Load() {
		return
	}
	d.inner.Accept()
}

func (d *DynamicClientRateLimiter) Wait(ctx context.Context) error {
	if !clientRateLimiterEnabled.Load() {
		return nil
	}
	return d.inner.Wait(ctx)
}

func (d *DynamicClientRateLimiter) Stop() {
	d.inner.Stop()
}

func (d *DynamicClientRateLimiter) QPS() float32 {
	if !clientRateLimiterEnabled.Load() {
		return float32(math.Inf(1))
	}
	return d.qps
}
