package main

import (
	"fmt"
	"net"
	"real-time-event-streaming/broker"
	"real-time-event-streaming/network"
)

func main() {

	b := broker.NewBroker()

	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		panic(err)
	}

	fmt.Println("Event Streaming Broker listening on port 9092")

	for {

		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}

		fmt.Println("New producer connected")

		go network.HandleConnection(conn, b)
	}
}
