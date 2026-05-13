package network

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"real-time-event-streaming/internal/broker"
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

			fmt.Println(
				"Client disconnected",
			)

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

			handleJoin(data, conn, b)

		} else if strings.HasPrefix(data, "COMMIT ") {

			handleCommit(data, conn, b)

		} else if strings.HasPrefix(data, "OFFSET ") {

			handleOffset(data, conn, b)

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

	value := messageParts[1]

	partition, offset :=
		b.Storage.Produce(
			topic,
			key,
			value,
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

	partition, _ := strconv.Atoi(
		parts[2],
	)

	offset, _ := strconv.Atoi(
		parts[3],
	)

	messages := b.Storage.Consume(
		topic,
		partition,
		offset,
	)

	for _, msg := range messages {

		line := fmt.Sprintf(
			"%d:%s\n",
			msg.Offset,
			msg.Value,
		)

		conn.Write([]byte(line))
	}
}

func handleJoin(
	data string,
	conn net.Conn,
	b *broker.Broker,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 4 {
		return
	}

	groupName := parts[1]

	topic := parts[2]

	consumerID := parts[3]

	partitions :=
		b.Coordinator.
			GroupManager.
			JoinGroup(
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

func handleCommit(
	data string,
	conn net.Conn,
	b *broker.Broker,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 5 {
		return
	}

	groupName := parts[1]

	topic := parts[2]

	partition, _ := strconv.Atoi(
		parts[3],
	)

	offset, _ := strconv.Atoi(
		parts[4],
	)

	b.Coordinator.
		OffsetManager.
		Commit(
			groupName,
			topic,
			partition,
			offset,
		)

	conn.Write([]byte(
		"COMMIT OK\n",
	))
}

func handleOffset(
	data string,
	conn net.Conn,
	b *broker.Broker,
) {

	parts := strings.Split(data, " ")

	if len(parts) != 4 {
		return
	}

	groupName := parts[1]

	topic := parts[2]

	partition, _ := strconv.Atoi(
		parts[3],
	)

	offset :=
		b.Coordinator.
			OffsetManager.
			GetOffset(
				groupName,
				topic,
				partition,
			)

	response := fmt.Sprintf(
		"%d\n",
		offset,
	)

	conn.Write([]byte(response))
}
