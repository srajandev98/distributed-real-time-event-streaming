package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ListenAddr    string
	DataDir       string
	NumPartitions int
}

func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:    getEnv("RTES_LISTEN_ADDR", ":9092"),
		DataDir:       getEnv("RTES_DATA_DIR", "data"),
		NumPartitions: 3,
	}

	if raw := os.Getenv("RTES_NUM_PARTITIONS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid RTES_NUM_PARTITIONS: %w", err)
		}
		cfg.NumPartitions = parsed
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

	return cfg, nil
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
