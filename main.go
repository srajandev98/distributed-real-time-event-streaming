package main

import (
	"fmt"
	"net"

	"real-time-event-streaming/broker"
	"real-time-event-streaming/group"
	"real-time-event-streaming/network"
)

func main() {

	b := broker.NewBroker()

	gm := group.NewGroupManager()

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

		fmt.Println(
			"New client connected",
		)

		go network.HandleConnection(
			conn,
			b,
			gm,
		)
	}
}
