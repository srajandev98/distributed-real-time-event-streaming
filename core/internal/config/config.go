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
	ReplicationFactor      int
	MinInSyncReplicas      int
	ReplicaMaxLag          int
	ReplicaLagTimeoutMs    int64
	AckAllTimeoutMs        int64
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
		ListenAddr:             getEnv("FLUX_LISTEN_ADDR", ":9092"),
		DataDir:                getEnv("FLUX_DATA_DIR", "data"),
		NumPartitions:          3,
		ReplicationFactor:      3,
		MinInSyncReplicas:      2,
		ReplicaMaxLag:          0,
		ReplicaLagTimeoutMs:    10000,
		AckAllTimeoutMs:        2000,
		SegmentMaxBytes:        1 * 1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}

	if raw := os.Getenv("FLUX_NUM_PARTITIONS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_NUM_PARTITIONS: %w", err)
		}
		cfg.NumPartitions = parsed
	}
	if raw := os.Getenv("FLUX_REPLICATION_FACTOR"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_REPLICATION_FACTOR: %w", err)
		}
		cfg.ReplicationFactor = parsed
	}
	if raw := os.Getenv("FLUX_MIN_ISR"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_MIN_ISR: %w", err)
		}
		cfg.MinInSyncReplicas = parsed
	}
	if raw := os.Getenv("FLUX_REPLICA_MAX_LAG"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_REPLICA_MAX_LAG: %w", err)
		}
		cfg.ReplicaMaxLag = parsed
	}
	if raw := os.Getenv("FLUX_REPLICA_LAG_TIMEOUT_MS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_REPLICA_LAG_TIMEOUT_MS: %w", err)
		}
		cfg.ReplicaLagTimeoutMs = parsed
	}
	if raw := os.Getenv("FLUX_ACK_ALL_TIMEOUT_MS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_ACK_ALL_TIMEOUT_MS: %w", err)
		}
		cfg.AckAllTimeoutMs = parsed
	}
	if raw := os.Getenv("FLUX_SEGMENT_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_SEGMENT_MAX_BYTES: %w", err)
		}
		cfg.SegmentMaxBytes = parsed
	}
	if raw := os.Getenv("FLUX_RETENTION_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_RETENTION_MAX_BYTES: %w", err)
		}
		cfg.RetentionMaxBytes = parsed
	}
	if raw := os.Getenv("FLUX_RETENTION_MAX_AGE_SECONDS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_RETENTION_MAX_AGE_SECONDS: %w", err)
		}
		cfg.RetentionMaxAgeSeconds = parsed
	}
	if raw := os.Getenv("FLUX_FLUSH_INTERVAL_MS"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_FLUSH_INTERVAL_MS: %w", err)
		}
		cfg.FlushIntervalMs = parsed
	}
	if raw := os.Getenv("FLUX_FLUSH_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLUX_FLUSH_BYTES: %w", err)
		}
		cfg.FlushBytes = parsed
	}
	if raw := os.Getenv("FLUX_FSYNC_MODE"); raw != "" {
		cfg.FsyncMode = raw
	}

	if cfg.NumPartitions <= 0 {
		return nil, fmt.Errorf("num partitions must be > 0")
	}
	if cfg.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("replication factor must be > 0")
	}
	if cfg.MinInSyncReplicas <= 0 {
		return nil, fmt.Errorf("min isr must be > 0")
	}
	if cfg.MinInSyncReplicas > cfg.ReplicationFactor {
		return nil, fmt.Errorf("min isr cannot exceed replication factor")
	}
	if cfg.ReplicaMaxLag < 0 {
		return nil, fmt.Errorf("replica max lag must be >= 0")
	}
	if cfg.ReplicaLagTimeoutMs <= 0 {
		return nil, fmt.Errorf("replica lag timeout must be > 0")
	}
	if cfg.AckAllTimeoutMs <= 0 {
		return nil, fmt.Errorf("ack all timeout must be > 0")
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
