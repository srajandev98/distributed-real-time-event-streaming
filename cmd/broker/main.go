package main

import (
	"net"
	"os"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/logging"
	"real-time-event-streaming/internal/network"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logging.Error("config load failed", "error", err)
		os.Exit(1)
	}

	b := broker.NewBroker(cfg)

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logging.Error("listener startup failed", "listen_addr", cfg.ListenAddr, "error", err)
		os.Exit(1)
	}

	logging.Info("broker started", "listen_addr", cfg.ListenAddr, "data_dir", cfg.DataDir, "num_partitions", cfg.NumPartitions)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logging.Warn("connection accept failed", "error", err)
			continue
		}

		logging.Info("client connected", "remote_addr", conn.RemoteAddr().String())
		go network.HandleConnection(conn, b)
	}
}
