package network

import (
	"fmt"
	"net"
	"real-time-event-streaming/broker"
	"strings"
)

func HandleConnection(conn net.Conn, b *broker.Broker) {

	defer conn.Close()

	buffer := make([]byte, 1024)

	for {

		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Client disconnected")
			return
		}

		data := string(buffer[:n])

		parts := strings.SplitN(data, ":", 2)

		if len(parts) != 2 {
			fmt.Println("Invalid message format")
			continue
		}

		topic := strings.TrimSpace(parts[0])
		message := strings.TrimSpace(parts[1])

		b.AddMessage(topic, message)
	}
}
