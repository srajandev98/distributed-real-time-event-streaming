package network

import (
	"fmt"
	"net"
	"strings"

	"real-time-event-streaming/broker"
)

func HandleConnection(
	conn net.Conn,
	b *broker.Broker,
) {

	defer conn.Close()

	buffer := make([]byte, 1024)

	for {

		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Client disconnected")
			return
		}

		data := strings.TrimSpace(
			string(buffer[:n]),
		)

		if strings.HasPrefix(data, "PRODUCE ") {

			handleProduce(data, b)

		} else if strings.HasPrefix(data, "CONSUME ") {

			handleConsume(data, conn, b)

		} else {

			conn.Write([]byte(
				"Invalid command\n",
			))
		}
	}
}

func handleProduce(
	data string,
	b *broker.Broker,
) {

	payload := strings.TrimPrefix(
		data,
		"PRODUCE ",
	)

	parts := strings.SplitN(payload, ":", 2)

	if len(parts) != 2 {
		fmt.Println("Invalid produce format")
		return
	}

	topic := strings.TrimSpace(parts[0])

	message := strings.TrimSpace(parts[1])

	offset := b.AddMessage(topic, message)

	fmt.Printf(
		"Produced topic=%s offset=%d\n",
		topic,
		offset,
	)
}

func handleConsume(
	data string,
	conn net.Conn,
	b *broker.Broker,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 3 {
		conn.Write([]byte(
			"Invalid consume format\n",
		))
		return
	}

	topic := parts[1]

	offset := broker.ParseOffset(parts[2])

	messages := b.Consume(topic, offset)

	for index, msg := range messages {

		line := fmt.Sprintf(
			"%d:%s\n",
			offset+index,
			msg,
		)

		conn.Write([]byte(line))
	}
}
