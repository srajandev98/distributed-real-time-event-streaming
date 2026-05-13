package main

import (
	"fmt"
	"net"

	"real-time-event-streaming/broker"
	"real-time-event-streaming/group"
	"real-time-event-streaming/network"
	"real-time-event-streaming/offset"
)

func main() {

	b := broker.NewBroker()

	gm := group.NewGroupManager()

	om := offset.NewOffsetManager()

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
			gm,
			om,
		)
	}
}
