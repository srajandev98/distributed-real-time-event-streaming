package network

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/logging"
	"real-time-event-streaming/internal/protocol"
)

func HandleConnection(conn net.Conn, b *broker.Broker) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		req, err := protocol.ParseRequest(line)
		if err != nil {
			logging.Warn("request parse failed", "remote_addr", remoteAddr, "error", err, "raw", line)
			conn.Write([]byte(protocol.Err("0", "BAD_REQUEST", err.Error())))
			continue
		}
		logging.Info("request received", "remote_addr", remoteAddr, "correlation_id", req.CorrelationID, "command", req.Command)

		response := handleRequest(req, b)
		conn.Write([]byte(response))
	}

	if err := scanner.Err(); err != nil {
		logging.Warn("client disconnected with scanner error", "remote_addr", remoteAddr, "error", err)
		return
	}
	logging.Info("client disconnected", "remote_addr", remoteAddr)
}

func handleRequest(req *protocol.Request, b *broker.Broker) string {
	switch req.Command {
	case "PRODUCE":
		return handleProduce(req, b)
	case "CONSUME":
		return handleConsume(req, b)
	case "JOIN":
		return handleJoin(req, b)
	case "COMMIT":
		return handleCommit(req, b)
	case "OFFSET":
		return handleOffset(req, b)
	default:
		return protocol.Err(req.CorrelationID, "UNKNOWN_COMMAND", "unsupported command")
	}
}

func handleProduce(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) < 2 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "PRODUCE requires: <topic> <key>:<value>")
	}

	topic := req.Args[0]
	messageParts := strings.SplitN(req.Args[1], ":", 2)
	if len(messageParts) != 2 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "message must be key:value")
	}

	key := messageParts[0]
	value := messageParts[1]
	partition, offset := b.Storage.Produce(topic, key, value)
	logging.Info("message produced", "correlation_id", req.CorrelationID, "topic", topic, "partition", partition, "offset", offset)

	payload := fmt.Sprintf("partition=%d offset=%d", partition, offset)
	return protocol.Ok(req.CorrelationID, payload)
}

func handleConsume(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 3 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "CONSUME requires: <topic> <partition> <offset>")
	}

	topic := req.Args[0]
	partition, err := protocol.ParseInt(req.Args[1], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	offset, err := protocol.ParseInt(req.Args[2], "offset")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	messages := b.Storage.Consume(topic, partition, offset)
	if len(messages) == 0 {
		return protocol.Ok(req.CorrelationID, "messages=")
	}

	items := make([]string, 0, len(messages))
	for _, msg := range messages {
		items = append(items, fmt.Sprintf("%d:%s", msg.Offset, msg.Value))
	}

	return protocol.Ok(req.CorrelationID, "messages="+strings.Join(items, ","))
}

func handleJoin(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 3 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "JOIN requires: <group> <topic> <consumer_id>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]

	partitions := b.Coordinator.GroupManager.JoinGroup(groupName, topic, consumerID)
	return protocol.Ok(req.CorrelationID, fmt.Sprintf("assigned=%v", partitions))
}

func handleCommit(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "COMMIT requires: <group> <topic> <partition> <offset>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	partition, err := protocol.ParseInt(req.Args[2], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	offset, err := protocol.ParseInt(req.Args[3], "offset")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	b.Coordinator.OffsetManager.Commit(groupName, topic, partition, offset)
	logging.Info("offset committed", "correlation_id", req.CorrelationID, "group", groupName, "topic", topic, "partition", partition, "offset", offset)
	return protocol.Ok(req.CorrelationID, "committed=true")
}

func handleOffset(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 3 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "OFFSET requires: <group> <topic> <partition>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	partition, err := protocol.ParseInt(req.Args[2], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	offset := b.Coordinator.OffsetManager.GetOffset(groupName, topic, partition)
	return protocol.Ok(req.CorrelationID, fmt.Sprintf("offset=%d", offset))
}
