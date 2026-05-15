package main

import (
	"fmt"
	"net"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/network"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	b := broker.NewBroker(cfg)

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Real-time Event Streaming Broker listening on %s\n", cfg.ListenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}

		go network.HandleConnection(conn, b)
	}
}
