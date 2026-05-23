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

// HandleConnection is the request loop for one client TCP connection.
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

// handleRequest routes parsed commands to command-specific handlers.
func handleRequest(req *protocol.Request, b *broker.Broker) string {
	switch req.Command {
	case "PRODUCE":
		return handleProduce(req, b)
	case "CONSUME":
		return handleConsume(req, b)
	case "JOIN":
		return handleJoin(req, b)
	case "SYNC":
		return handleSync(req, b)
	case "HEARTBEAT":
		return handleHeartbeat(req, b)
	case "LEAVE":
		return handleLeave(req, b)
	case "COMMIT":
		return handleCommit(req, b)
	case "OFFSET":
		return handleOffset(req, b)
	case "REPLICA_FETCH":
		return handleReplicaFetch(req, b)
	case "SET_PARTITION_ROLE":
		return handleSetPartitionRole(req, b)
	default:
		return protocol.Err(req.CorrelationID, "UNKNOWN_COMMAND", "unsupported command")
	}
}

// handleProduce validates produce args and appends a message to storage.
func handleProduce(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) < 2 || len(req.Args) > 3 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "PRODUCE requires: <topic> <key>:<value> [acks=0|1|all]")
	}

	topic := req.Args[0]
	messageParts := strings.SplitN(req.Args[1], ":", 2)
	if len(messageParts) != 2 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "message must be key:value")
	}

	key := messageParts[0]
	value := messageParts[1]

	ackMode := "1"
	if len(req.Args) == 3 {
		parts := strings.SplitN(req.Args[2], "=", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "acks" {
			return protocol.Err(req.CorrelationID, "BAD_REQUEST", "invalid ack mode; expected acks=0|1|all")
		}
		ackMode = strings.ToLower(parts[1])
		if ackMode != "0" && ackMode != "1" && ackMode != "all" {
			return protocol.Err(req.CorrelationID, "BAD_REQUEST", "invalid ack mode; expected acks=0|1|all")
		}
	}

	partition := b.Storage.PartitionForKey(key)
	if !b.Replication.IsLeader(topic, partition) {
		return protocol.Err(req.CorrelationID, "NOT_LEADER", "partition leader unavailable on this broker")
	}

	partition, offset := b.Storage.Produce(topic, key, value)
	b.Replication.OnLeaderAppend(topic, partition, offset)

	if ackMode == "all" {
		if err := b.Replication.WaitForAckAll(topic, partition, offset); err != nil {
			return protocol.Err(req.CorrelationID, "REPLICATION_TIMEOUT", err.Error())
		}
	}

	logging.Info("message produced", "correlation_id", req.CorrelationID, "topic", topic, "partition", partition, "offset", offset)

	payload := fmt.Sprintf("partition=%d offset=%d hw=%d", partition, offset, b.Replication.HighWatermark(topic, partition))
	return protocol.Ok(req.CorrelationID, payload)
}

func handleSetPartitionRole(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 3 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "SET_PARTITION_ROLE requires: <topic> <partition> <leader|follower>")
	}

	topic := req.Args[0]
	partition, err := protocol.ParseInt(req.Args[1], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	role := strings.ToLower(req.Args[2])
	if err := b.Replication.SetRole(topic, partition, role); err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	status := b.Replication.Status(topic, partition)
	return protocol.Ok(req.CorrelationID, fmt.Sprintf("topic=%s partition=%d role=%s hw=%d", topic, partition, status.Role, status.HighWatermark))
}

// handleConsume validates consume args and returns messages from an offset.
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

	highWatermark := b.Replication.HighWatermark(topic, partition)
	if highWatermark < 0 || offset > highWatermark {
		return protocol.Ok(req.CorrelationID, fmt.Sprintf("messages= hw=%d", highWatermark))
	}

	messages := b.Storage.Consume(topic, partition, offset)
	filtered := make([]string, 0, len(messages))
	for _, msg := range messages {
		if msg.Offset > highWatermark {
			break
		}
		filtered = append(filtered, fmt.Sprintf("%d:%s", msg.Offset, msg.Value))
	}

	if len(filtered) == 0 {
		return protocol.Ok(req.CorrelationID, fmt.Sprintf("messages= hw=%d", highWatermark))
	}

	if len(messages) == 0 {
		return protocol.Ok(req.CorrelationID, fmt.Sprintf("messages= hw=%d", highWatermark))
	}
	return protocol.Ok(req.CorrelationID, "messages="+strings.Join(filtered, ",")+fmt.Sprintf(" hw=%d", highWatermark))
}

// handleJoin registers a consumer into a group and returns assignments.
func handleJoin(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 3 && len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "JOIN requires: <group> <topic> <consumer_id> [assignor=round_robin|range]")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]
	assignor := ""
	if len(req.Args) == 4 {
		parts := strings.SplitN(req.Args[3], "=", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "assignor" {
			return protocol.Err(req.CorrelationID, "BAD_REQUEST", "invalid assignor; expected assignor=round_robin|range")
		}
		assignor = strings.ToLower(parts[1])
		if assignor != "round_robin" && assignor != "range" {
			return protocol.Err(req.CorrelationID, "BAD_REQUEST", "invalid assignor; expected assignor=round_robin|range")
		}
	}

	result, err := b.Coordinator.GroupManager.JoinGroup(groupName, topic, consumerID, assignor)
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	return protocol.Ok(req.CorrelationID, fmt.Sprintf("generation=%d assigned=%v", result.Generation, result.Assigned))
}

func handleSync(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "SYNC requires: <group> <topic> <consumer_id> <generation>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]
	generation, err := protocol.ParseInt(req.Args[3], "generation")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	result, err := b.Coordinator.GroupManager.SyncGroup(groupName, topic, consumerID, generation)
	if err != nil {
		return protocol.Err(req.CorrelationID, "GENERATION_MISMATCH", err.Error())
	}
	return protocol.Ok(req.CorrelationID, fmt.Sprintf("generation=%d assigned=%v", result.Generation, result.Assigned))
}

func handleHeartbeat(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "HEARTBEAT requires: <group> <topic> <consumer_id> <generation>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]
	generation, err := protocol.ParseInt(req.Args[3], "generation")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	if err := b.Coordinator.GroupManager.Heartbeat(groupName, topic, consumerID, generation); err != nil {
		return protocol.Err(req.CorrelationID, "GENERATION_MISMATCH", err.Error())
	}
	return protocol.Ok(req.CorrelationID, "heartbeat=ok")
}

func handleLeave(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "LEAVE requires: <group> <topic> <consumer_id> <generation>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]
	generation, err := protocol.ParseInt(req.Args[3], "generation")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	if err := b.Coordinator.GroupManager.LeaveGroup(groupName, topic, consumerID, generation); err != nil {
		return protocol.Err(req.CorrelationID, "GENERATION_MISMATCH", err.Error())
	}
	return protocol.Ok(req.CorrelationID, "left=true")
}

// handleCommit stores processed offsets for group progress tracking.
func handleCommit(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 6 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "COMMIT requires: <group> <topic> <consumer_id> <generation> <partition> <offset>")
	}

	groupName := req.Args[0]
	topic := req.Args[1]
	consumerID := req.Args[2]
	generation, err := protocol.ParseInt(req.Args[3], "generation")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	partition, err := protocol.ParseInt(req.Args[4], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	offset, err := protocol.ParseInt(req.Args[5], "offset")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	if err := b.Coordinator.GroupManager.ValidateCommit(groupName, topic, consumerID, generation, partition); err != nil {
		return protocol.Err(req.CorrelationID, "GENERATION_MISMATCH", err.Error())
	}
	b.Coordinator.OffsetManager.Commit(groupName, topic, partition, offset)
	logging.Info("offset committed", "correlation_id", req.CorrelationID, "group", groupName, "topic", topic, "consumer_id", consumerID, "generation", generation, "partition", partition, "offset", offset)
	return protocol.Ok(req.CorrelationID, "committed=true")
}

// handleOffset returns last committed offset for a group/topic/partition.
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

// handleReplicaFetch simulates follower fetch/ack progress for a partition.
func handleReplicaFetch(req *protocol.Request, b *broker.Broker) string {
	if len(req.Args) != 4 {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", "REPLICA_FETCH requires: <topic> <partition> <replica_id> <offset>")
	}

	topic := req.Args[0]
	partition, err := protocol.ParseInt(req.Args[1], "partition")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	replicaID, err := protocol.ParseInt(req.Args[2], "replica_id")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	offset, err := protocol.ParseInt(req.Args[3], "offset")
	if err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}

	if err := b.Replication.AckReplica(topic, partition, replicaID, offset); err != nil {
		return protocol.Err(req.CorrelationID, "BAD_REQUEST", err.Error())
	}
	status := b.Replication.Status(topic, partition)
	return protocol.Ok(
		req.CorrelationID,
		fmt.Sprintf("replica=%d acked_offset=%d hw=%d isr=%v under_replicated=%t",
			replicaID,
			offset,
			status.HighWatermark,
			status.InSyncReplicas,
			status.UnderReplicated,
		),
	)
}
