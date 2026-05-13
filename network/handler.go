package network

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"real-time-event-streaming/broker"
	"real-time-event-streaming/group"
)

func HandleConnection(
	conn net.Conn,
	b *broker.Broker,
	gm *group.GroupManager,
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

		} else if strings.HasPrefix(data, "JOIN ") {

			handleJoin(data, conn, gm)

		} else {

			conn.Write([]byte(
				"Invalid command\n",
			))
		}
	}
}

func handleJoin(
	data string,
	conn net.Conn,
	gm *group.GroupManager,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 4 {

		conn.Write([]byte(
			"Invalid JOIN format\n",
		))

		return
	}

	groupName := parts[1]

	topic := parts[2]

	consumerID := parts[3]

	partitions := gm.JoinGroup(
		groupName,
		topic,
		consumerID,
	)

	response := fmt.Sprintf(
		"ASSIGNED %v\n",
		partitions,
	)

	conn.Write([]byte(response))
}

func handleProduce(
	data string,
	b *broker.Broker,
) {

	payload := strings.TrimPrefix(
		data,
		"PRODUCE ",
	)

	parts := strings.SplitN(
		payload,
		" ",
		2,
	)

	if len(parts) != 2 {
		return
	}

	topic := parts[0]

	messageParts := strings.SplitN(
		parts[1],
		":",
		2,
	)

	if len(messageParts) != 2 {
		return
	}

	key := messageParts[0]

	message := messageParts[1]

	partition, offset := b.Produce(
		topic,
		key,
		message,
	)

	fmt.Printf(
		"Produced topic=%s partition=%d offset=%d\n",
		topic,
		partition,
		offset,
	)
}

func handleConsume(
	data string,
	conn net.Conn,
	b *broker.Broker,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 4 {
		return
	}

	topic := parts[1]

	partition, err := strconv.Atoi(
		parts[2],
	)

	if err != nil {
		return
	}

	offset, err := strconv.Atoi(
		parts[3],
	)

	if err != nil {
		return
	}

	messages := b.Consume(
		topic,
		partition,
		offset,
	)

	for index, msg := range messages {

		line := fmt.Sprintf(
			"%d:%s\n",
			offset+index,
			msg,
		)

		conn.Write([]byte(line))
	}
}
