package main

import (
	"net"
	"os"

	"flux/internal/broker"
	"flux/internal/config"
	"flux/internal/logging"
	"flux/internal/network"
)

// main boots config, builds all broker dependencies, and starts the TCP server.
func main() {
	cfg, err := config.Load()
	if err != nil {
		logging.Error("config load failed", "error", err)
		os.Exit(1)
	}

	b := broker.NewBroker(cfg)
	b.StartBackgroundRuntimes()
	defer b.StopBackgroundRuntimes()

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logging.Error("listener startup failed", "listen_addr", cfg.ListenAddr, "error", err)
		os.Exit(1)
	}

	logging.Info("broker started", "listen_addr", cfg.ListenAddr, "data_dir", cfg.DataDir, "num_partitions", cfg.NumPartitions)

	for {
		// Each accepted connection is handled in a separate goroutine so
		// multiple clients can produce/consume at the same time.
		conn, err := listener.Accept()
		if err != nil {
			logging.Warn("connection accept failed", "error", err)
			continue
		}

		logging.Info("client connected", "remote_addr", conn.RemoteAddr().String())
		go network.HandleConnection(conn, b)
	}
}
