// Package hipserver implements the HIP/1 TCP ingest server.
package hipserver

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
)

// Config is the runtime tunables. AllowedServers is checked case-sensitively.
type Config struct {
	Addr               string
	TLSCert            string
	TLSKey             string
	BearerToken        string
	MaxFrame           uint64
	MaxInFlightUploads uint32
	AllowedServers     map[string]struct{}
	ServerVersion      string
}

// Metrics is an optional hook set. All fields are nil-safe.
type Metrics struct {
	SessionsActive func(delta int)
	SessionsTotal  func(result string) // "commit" or "abort"
	Uploads        func(status string) // "ok", "sha_mismatch", ...
	BytesIngested  func(delta uint64)
}

type CacheInvalidator interface {
	InvalidatePaths(paths []string)
}

// Server owns the TCP listener and per-connection goroutines.
type Server struct {
	cfg     Config
	db      *sql.DB
	storage *storage.Client
	cache   CacheInvalidator
	metrics *Metrics
	log     *slog.Logger

	ln     net.Listener
	closed chan struct{}
	wg     sync.WaitGroup
}

// New wires the dependencies.
func New(cfg Config, db *sql.DB, sc *storage.Client, cache CacheInvalidator, m *Metrics, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.MaxFrame == 0 {
		cfg.MaxFrame = 16 * 1024 * 1024
	}
	if cfg.MaxInFlightUploads == 0 {
		cfg.MaxInFlightUploads = 8
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "moe-assets-gateway/1"
	}
	return &Server{
		cfg:     cfg,
		db:      db,
		storage: sc,
		cache:   cache,
		metrics: m,
		log:     log,
		closed:  make(chan struct{}),
	}
}

// ListenAndServe binds and runs the accept loop until ctx is cancelled or
// Shutdown is called.
func (s *Server) ListenAndServe(ctx context.Context) error {
	var ln net.Listener
	if s.cfg.TLSCert != "" && s.cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("hipserver: load tls: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		l, err := tls.Listen("tcp", s.cfg.Addr, tlsCfg)
		if err != nil {
			return err
		}
		ln = l
	} else {
		l, err := net.Listen("tcp", s.cfg.Addr)
		if err != nil {
			return err
		}
		ln = l
	}
	s.ln = ln
	s.log.Info("hip: listening", "addr", s.cfg.Addr, "tls", s.cfg.TLSCert != "")

	// Fanout: cancel ctx → close listener.
	go func() {
		<-ctx.Done()
		_ = s.ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			s.log.Warn("hip: accept error", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.runSession(ctx, c)
		}(conn)
	}
	// Wait for outstanding sessions.
	s.wg.Wait()
	close(s.closed)
	return nil
}

// Shutdown closes the listener; existing sessions run to completion (or ctx
// cancellation, whichever comes first).
func (s *Server) Shutdown() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// Addr returns the bound address (post-Listen).
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) serverAllowed(region string) bool {
	_, ok := s.cfg.AllowedServers[region]
	return ok
}

func (s *Server) gaugeSessionsActive(delta int) {
	if s.metrics != nil && s.metrics.SessionsActive != nil {
		s.metrics.SessionsActive(delta)
	}
}
func (s *Server) counterSessions(result string) {
	if s.metrics != nil && s.metrics.SessionsTotal != nil {
		s.metrics.SessionsTotal(result)
	}
}
func (s *Server) counterUploads(status string) {
	if s.metrics != nil && s.metrics.Uploads != nil {
		s.metrics.Uploads(status)
	}
}
func (s *Server) counterBytesIngested(n uint64) {
	if s.metrics != nil && s.metrics.BytesIngested != nil {
		s.metrics.BytesIngested(n)
	}
}

// newSessionID returns a short random id (not a full ULID to avoid an extra
// dep; 16 hex chars is plenty for logging correlation).
func newSessionID() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
