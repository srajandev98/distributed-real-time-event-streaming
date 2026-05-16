package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config contains all runtime knobs needed by broker modules.
type Config struct {
	ListenAddr             string
	DataDir                string
	NumPartitions          int
	SegmentMaxBytes        int64
	RetentionMaxBytes      int64
	RetentionMaxAgeSeconds int64
	FlushIntervalMs        int64
	FlushBytes             int64
	FsyncMode              string
}

// Load builds config from defaults + environment variables and validates it.
func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:             getEnv("RTES_LISTEN_ADDR", ":9092"),
		DataDir:                getEnv("RTES_DATA_DIR", "data"),
		NumPartitions:          3,
		SegmentMaxBytes:        1 * 1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}

	if raw := os.Getenv("RTES_NUM_PARTITIONS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_NUM_PARTITIONS: %w", err)
		}
		cfg.NumPartitions = parsed
	}
	if raw := os.Getenv("RTES_SEGMENT_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_SEGMENT_MAX_BYTES: %w", err)
		}
		cfg.SegmentMaxBytes = parsed
	}
	if raw := os.Getenv("RTES_RETENTION_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_RETENTION_MAX_BYTES: %w", err)
		}
		cfg.RetentionMaxBytes = parsed
	}
	if raw := os.Getenv("RTES_RETENTION_MAX_AGE_SECONDS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_RETENTION_MAX_AGE_SECONDS: %w", err)
		}
		cfg.RetentionMaxAgeSeconds = parsed
	}
	if raw := os.Getenv("RTES_FLUSH_INTERVAL_MS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_FLUSH_INTERVAL_MS: %w", err)
		}
		cfg.FlushIntervalMs = parsed
	}
	if raw := os.Getenv("RTES_FLUSH_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_FLUSH_BYTES: %w", err)
		}
		cfg.FlushBytes = parsed
	}
	if raw := os.Getenv("RTES_FSYNC_MODE"); raw != "" {
		cfg.FsyncMode = raw
	}

	if cfg.NumPartitions <= 0 {
		return nil, fmt.Errorf("num partitions must be > 0")
	}

	if cfg.ListenAddr == "" {
		return nil, fmt.Errorf("listen addr cannot be empty")
	}

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir cannot be empty")
	}
	if cfg.SegmentMaxBytes <= 0 {
		return nil, fmt.Errorf("segment max bytes must be > 0")
	}
	if cfg.RetentionMaxBytes <= 0 {
		return nil, fmt.Errorf("retention max bytes must be > 0")
	}
	if cfg.RetentionMaxAgeSeconds <= 0 {
		return nil, fmt.Errorf("retention max age seconds must be > 0")
	}
	if cfg.FlushIntervalMs <= 0 {
		return nil, fmt.Errorf("flush interval ms must be > 0")
	}
	if cfg.FlushBytes <= 0 {
		return nil, fmt.Errorf("flush bytes must be > 0")
	}
	if cfg.FsyncMode != "always" && cfg.FsyncMode != "interval" && cfg.FsyncMode != "never" {
		return nil, fmt.Errorf("fsync mode must be one of: always, interval, never")
	}

	return cfg, nil
}

// getEnv returns env value when present; otherwise it uses fallback.
func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
