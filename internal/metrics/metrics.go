// Package metrics is a tiny dependency-free Prometheus-compatible text exporter.
//
// It supports labelled counters and gauges only, which is sufficient for this
// service. All series live in a single Registry.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry is a threadsafe collection of counters and gauges.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*Counter
	gauges   map[string]*Gauge
}

// NewRegistry constructs an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters: map[string]*Counter{},
		gauges:   map[string]*Gauge{},
	}
}

// Counter is a monotonic uint64 counter keyed by a label map.
type Counter struct {
	name   string
	help   string
	labels []string
	mu     sync.RWMutex
	series map[string]*uint64
}

// Gauge is a single arithmetic float value, updated atomically-ish under a
// registry-wide lock. Simplicity over throughput.
type Gauge struct {
	name string
	help string
	val  uint64 // stored as bit pattern of float64 via math.Float64bits
}

// NewCounter registers (or reuses) a counter series family.
func (r *Registry) NewCounter(name, help string, labels ...string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{
		name:   name,
		help:   help,
		labels: append([]string(nil), labels...),
		series: map[string]*uint64{},
	}
	r.counters[name] = c
	return c
}

// NewGauge registers (or reuses) a gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{name: name, help: help}
	r.gauges[name] = g
	return g
}

// Inc adds delta to the series identified by the ordered label values. The
// slice length must equal the counter's registered label count.
func (c *Counter) Inc(delta uint64, labelValues ...string) {
	key := c.labelKey(labelValues)
	c.mu.RLock()
	p, ok := c.series[key]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		if p, ok = c.series[key]; !ok {
			var zero uint64
			p = &zero
			c.series[key] = p
		}
		c.mu.Unlock()
	}
	atomic.AddUint64(p, delta)
}

func (c *Counter) labelKey(values []string) string {
	if len(c.labels) == 0 || len(values) == 0 {
		return ""
	}
	var b strings.Builder
	for i, name := range c.labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(name)
		b.WriteString(`="`)
		if i < len(values) {
			for _, r := range values[i] {
				switch r {
				case '\\':
					b.WriteString(`\\`)
				case '"':
					b.WriteString(`\"`)
				case '\n':
					b.WriteString(`\n`)
				default:
					b.WriteRune(r)
				}
			}
		}
		b.WriteByte('"')
	}
	return b.String()
}

// Add is a signed add mapped onto uint64 wrap semantics.
func (g *Gauge) Add(delta int64) {
	for {
		old := atomic.LoadUint64(&g.val)
		newVal := uint64(int64(old) + delta)
		if atomic.CompareAndSwapUint64(&g.val, old, newVal) {
			return
		}
	}
}

// Set stores an absolute value.
func (g *Gauge) Set(v int64) { atomic.StoreUint64(&g.val, uint64(v)) }

// Handler implements http.Handler.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Render(w)
	})
}

// Render emits Prometheus text format to w. Named Render (not WriteTo) so we
// don't accidentally implement io.WriterTo with a non-standard signature.
func (r *Registry) Render(w io.Writer) {
	r.mu.RLock()
	// Snapshot names for deterministic ordering.
	cnames := make([]string, 0, len(r.counters))
	for n := range r.counters {
		cnames = append(cnames, n)
	}
	gnames := make([]string, 0, len(r.gauges))
	for n := range r.gauges {
		gnames = append(gnames, n)
	}
	r.mu.RUnlock()
	sort.Strings(cnames)
	sort.Strings(gnames)

	for _, n := range cnames {
		c := r.counters[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
		c.mu.RLock()
		keys := make([]string, 0, len(c.series))
		for k := range c.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := atomic.LoadUint64(c.series[k])
			if k == "" {
				fmt.Fprintf(w, "%s %d\n", c.name, v)
			} else {
				fmt.Fprintf(w, "%s{%s} %d\n", c.name, k, v)
			}
		}
		c.mu.RUnlock()
	}

	for _, n := range gnames {
		g := r.gauges[n]
		val := int64(atomic.LoadUint64(&g.val))
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", g.name, g.help, g.name, g.name, val)
	}
}


