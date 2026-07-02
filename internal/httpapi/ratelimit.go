package httpapi

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// IPRateLimiter caps per-remote-IP request rate. Buckets are held in a plain
// map with periodic GC to avoid unbounded growth.
type IPRateLimiter struct {
	rps    rate.Limit
	burst  int
	mu     sync.Mutex
	seen   map[string]*rate.Limiter
	lastGC time.Time
}

// NewIPRateLimiter builds a limiter with (rps, burst). Set rps<=0 to disable.
func NewIPRateLimiter(rps float64, burst int) *IPRateLimiter {
	return &IPRateLimiter{
		rps:    rate.Limit(rps),
		burst:  burst,
		seen:   make(map[string]*rate.Limiter, 1024),
		lastGC: time.Now(),
	}
}

// getter returns (or creates) the limiter for one IP.
func (l *IPRateLimiter) getter(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lim, ok := l.seen[ip]; ok {
		return lim
	}
	lim := rate.NewLimiter(l.rps, l.burst)
	l.seen[ip] = lim
	// Cheap GC: if the map got big, drop everything and let it repopulate.
	if len(l.seen) > 16384 && time.Since(l.lastGC) > time.Minute {
		l.seen = make(map[string]*rate.Limiter, 1024)
		l.seen[ip] = lim
		l.lastGC = time.Now()
	}
	return lim
}

// Middleware wraps a handler. Disabled when rps<=0.
func (l *IPRateLimiter) Middleware(next http.Handler) http.Handler {
	if float64(l.rps) <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !l.getter(ip).Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP picks the best-effort client IP. Prefers X-Forwarded-For's first
// value when present (Zeabur ingress is upstream of the gateway).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return trimSpace(xff[:i])
			}
		}
		return trimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
