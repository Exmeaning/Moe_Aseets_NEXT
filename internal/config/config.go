// Package config parses env-based 12-factor configuration for the gateway.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is the fully validated runtime configuration.
type Config struct {
	HTTPAddr string
	HIPAddr  string

	HIPTLSCert     string
	HIPTLSKey      string
	HIPBearerToken string

	MaxFrameBytes       uint64
	MaxInFlightUploads  uint32
	SeaweedFilerBaseURL string
	SQLitePath          string

	HTTPRateLimitRPS   float64
	HTTPRateLimitBurst int

	AllowedServers map[string]struct{}

	LogLevel string
}

// FromEnv reads all HARUKI_GW_* variables and returns a validated Config.
func FromEnv() (Config, error) {
	c := Config{
		HTTPAddr:            envDefault("HARUKI_GW_HTTP_ADDR", ":8080"),
		HIPAddr:             envDefault("HARUKI_GW_HIP_ADDR", "127.0.0.1:7420"),
		HIPTLSCert:          os.Getenv("HARUKI_GW_HIP_TLS_CERT"),
		HIPTLSKey:           os.Getenv("HARUKI_GW_HIP_TLS_KEY"),
		HIPBearerToken:      os.Getenv("HARUKI_GW_HIP_BEARER_TOKEN"),
		SeaweedFilerBaseURL: envDefault("HARUKI_GW_SEAWEED_FILER", "http://seaweedfs-filer:8888"),
		SQLitePath:          envDefault("HARUKI_GW_SQLITE_PATH", "/data/gateway.db"),
		LogLevel:            envDefault("HARUKI_GW_LOG_LEVEL", "info"),
	}

	var err error
	c.MaxFrameBytes, err = envUint64("HARUKI_GW_MAX_FRAME_BYTES", 16*1024*1024)
	if err != nil {
		return c, err
	}
	inflight, err := envUint64("HARUKI_GW_MAX_INFLIGHT_UPLOADS", 8)
	if err != nil {
		return c, err
	}
	c.MaxInFlightUploads = uint32(inflight)
	c.HTTPRateLimitRPS, err = envFloat64("HARUKI_GW_HTTP_RATE_LIMIT_RPS", 200)
	if err != nil {
		return c, err
	}
	burst, err := envUint64("HARUKI_GW_HTTP_RATE_LIMIT_BURST", 400)
	if err != nil {
		return c, err
	}
	c.HTTPRateLimitBurst = int(burst)

	servers := envDefault("HARUKI_GW_ALLOWED_SERVERS", "jp,en,tw,kr,cn")
	c.AllowedServers = map[string]struct{}{}
	for _, s := range strings.Split(servers, ",") {
		s = strings.TrimSpace(strings.ToLower(s))
		if s != "" {
			c.AllowedServers[s] = struct{}{}
		}
	}

	if c.HIPBearerToken == "" {
		return c, errors.New("HARUKI_GW_HIP_BEARER_TOKEN is required (empty bearer would reject every session)")
	}
	if len(c.AllowedServers) == 0 {
		return c, errors.New("HARUKI_GW_ALLOWED_SERVERS must contain at least one entry")
	}
	if c.MaxFrameBytes < 4096 {
		return c, errors.New("HARUKI_GW_MAX_FRAME_BYTES must be >= 4096")
	}
	return c, nil
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envUint64(k string, def uint64) (uint64, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func envFloat64(k string, def float64) (float64, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}
