package hipserver

import (
	"context"
	"io"
	"sync"

	"github.com/Team-Haruki/moe-assets-gateway/internal/hipproto"
)

// frameWriter serialises frame emission on one TCP connection: any goroutine
// may enqueue a frame, but only one goroutine actually writes to the wire.
// This is required because the HIP spec allows interleaved processing of
// CHECK / UPLOAD requests but demands ordered frame emission per session.
type frameWriter struct {
	w        io.Writer
	maxFrame uint64
	ch       chan hipproto.Frame
	done     chan struct{}
	errMu    sync.Mutex
	err      error
	wg       sync.WaitGroup
}

func newFrameWriter(w io.Writer, maxFrame uint64, buffer int) *frameWriter {
	if buffer <= 0 {
		buffer = 32
	}
	fw := &frameWriter{
		w:        w,
		maxFrame: maxFrame,
		ch:       make(chan hipproto.Frame, buffer),
		done:     make(chan struct{}),
	}
	fw.wg.Add(1)
	go fw.loop()
	return fw
}

func (fw *frameWriter) loop() {
	defer fw.wg.Done()
	defer close(fw.done)
	for f := range fw.ch {
		if err := hipproto.WriteFrame(fw.w, f, fw.maxFrame); err != nil {
			fw.setErr(err)
			// drain remainder without writing
			for range fw.ch {
			}
			return
		}
	}
}

// Send enqueues a frame. Returns ctx error if the ctx cancels first.
func (fw *frameWriter) Send(ctx context.Context, f hipproto.Frame) error {
	select {
	case fw.ch <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-fw.done:
		return fw.Err()
	}
}

// Close signals no more sends. Waits for the writer loop to drain.
func (fw *frameWriter) Close() {
	close(fw.ch)
	fw.wg.Wait()
}

// Err returns the first write error observed (if any).
func (fw *frameWriter) Err() error {
	fw.errMu.Lock()
	defer fw.errMu.Unlock()
	return fw.err
}

func (fw *frameWriter) setErr(err error) {
	fw.errMu.Lock()
	if fw.err == nil {
		fw.err = err
	}
	fw.errMu.Unlock()
}
