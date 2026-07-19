// moe-assets-gateway is a single-binary Go service that exposes:
//   - a public read-only HTTP reverse proxy on /sekai-{server}-assets/{path...}
//   - a private HIP/1 TCP ingest port for the Haruki asset updater
//
// It talks to SeaweedFS (filer HTTP API) for object bytes and keeps an
// authoritative SQLite metadata store plus an in-memory placement index.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/config"
	"github.com/Team-Haruki/moe-assets-gateway/internal/hipserver"
	"github.com/Team-Haruki/moe-assets-gateway/internal/httpapi"
	"github.com/Team-Haruki/moe-assets-gateway/internal/metrics"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	log := buildLogger(cfg.LogLevel)
	slog.SetDefault(log)
	log.Info("moe-assets-gateway starting",
		"http_addr", cfg.HTTPAddr,
		"hip_addr", cfg.HIPAddr,
		"seaweed", cfg.SeaweedFilerBaseURL,
		"sqlite", cfg.SQLitePath,
	)

	// Ensure SQLite parent dir exists.
	if cfg.SQLitePath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(cfg.SQLitePath), 0o755); err != nil {
			return err
		}
	}

	db, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return err
	}
	defer db.Close()

	sc := storage.New(cfg.SeaweedFilerBaseURL)
	// Warm-up ping (best-effort).
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := sc.Ping(pingCtx); err != nil {
		log.Warn("seaweedfs ping failed at startup", "err", err)
	}
	pingCancel()

	indexCtx, indexCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	indexStart := time.Now()
	log.Info("read index: ensuring materialized sqlite tables")
	if err := store.EnsureReadIndexes(indexCtx, db); err != nil {
		indexCancel()
		return err
	}
	indexCancel()
	log.Info("read index: ready", "elapsed_ms", time.Since(indexStart).Milliseconds())

	// ---- Metrics registry ----
	reg := metrics.NewRegistry()
	metHTTPReq := reg.NewCounter("http_requests_total", "HTTP read requests", "server", "result")
	metHTTPBytes := reg.NewCounter("http_bytes_out_total", "HTTP bytes served to clients")
	metHIPActive := reg.NewGauge("hip_sessions_active", "Currently open HIP sessions")
	metHIPSessions := reg.NewCounter("hip_sessions_total", "Completed HIP sessions", "result")
	metHIPUploads := reg.NewCounter("hip_uploads_total", "HIP upload attempts", "status")
	metHIPBytes := reg.NewCounter("hip_bytes_ingested_total", "Bytes received via HIP UPLOAD_CHUNK")

	proxyMetrics := &httpapi.ProxyMetrics{
		RequestsTotal: func(server, result string) { metHTTPReq.Inc(1, server, result) },
		BytesOut:      func(n int64) { metHTTPBytes.Inc(uint64(n)) },
	}
	hipMetrics := &hipserver.Metrics{
		SessionsActive: func(delta int) { metHIPActive.Add(int64(delta)) },
		SessionsTotal:  func(result string) { metHIPSessions.Inc(1, result) },
		Uploads:        func(status string) { metHIPUploads.Inc(1, status) },
		BytesIngested:  func(n uint64) { metHIPBytes.Inc(n) },
	}

	// ---- HTTP read port ----
	lookupCache := httpapi.NewLookupCache(
		cfg.LookupCacheItems,
		time.Duration(cfg.LookupCacheTTL)*time.Second,
	)
	proxy := &httpapi.ProxyHandler{
		DB:             db,
		Storage:        sc,
		Log:            log,
		AllowedServers: cfg.AllowedServers,
		Metrics:        proxyMetrics,
		Cache:          lookupCache,
	}
	browser := &httpapi.AssetBrowserHandler{
		DB:             db,
		AllowedServers: cfg.AllowedServers,
	}
	versions := &httpapi.AssetVersionsHandler{
		DB:             db,
		AllowedServers: cfg.AllowedServers,
	}
	router := &httpapi.Router{
		Proxy:    proxy,
		Browser:  browser,
		Versions: versions,
		Metrics:  reg.Handler(),
		Limiter:  httpapi.NewIPRateLimiter(cfg.HTTPRateLimitRPS, cfg.HTTPRateLimitBurst),
	}
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0, // large downloads
	}

	// ---- HIP ingest port ----
	hipSrv := hipserver.New(hipserver.Config{
		Addr:               cfg.HIPAddr,
		TLSCert:            cfg.HIPTLSCert,
		TLSKey:             cfg.HIPTLSKey,
		BearerToken:        cfg.HIPBearerToken,
		MaxFrame:           cfg.MaxFrameBytes,
		MaxInFlightUploads: cfg.MaxInFlightUploads,
		AllowedServers:     cfg.AllowedServers,
		ServerVersion:      "moe-assets-gateway/1",
	}, db, sc, lookupCache, hipMetrics, log)

	// ---- run loop with signal handling ----
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		log.Info("http: listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	go func() {
		if err := hipSrv.ListenAndServe(rootCtx); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-rootCtx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			log.Error("component exited", "err", err)
		}
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	_ = httpSrv.Shutdown(shutCtx)
	_ = hipSrv.Shutdown()
	// Wait remaining component to exit.
	<-errCh
	log.Info("moe-assets-gateway stopped")
	return nil
}

func buildLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
