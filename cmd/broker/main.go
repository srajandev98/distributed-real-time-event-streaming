package main

import (
	"fmt"
	"net"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/network"
)

func main() {

	b := broker.NewBroker()

	listener, err := net.Listen(
		"tcp",
		":9092",
	)

	if err != nil {
		panic(err)
	}

	fmt.Println(
		"Real-time Event Streaming Broker listening on port 9092",
	)

	for {

		conn, err := listener.Accept()

		if err != nil {

			fmt.Println(
				"Connection error:",
				err,
			)

			continue
		}

		go network.HandleConnection(
			conn,
			b,
		)
	}
}
