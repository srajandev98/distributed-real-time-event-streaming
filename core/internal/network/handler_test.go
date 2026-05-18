package network

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/protocol"
)

func parsePartitionFromProduceResponse(resp string) (int, error) {
	idx := strings.Index(resp, "partition=")
	if idx == -1 {
		return 0, fmt.Errorf("partition field missing")
	}
	rest := resp[idx+len("partition="):]
	end := strings.Index(rest, " ")
	if end == -1 {
		end = len(rest)
	}
	return strconv.Atoi(rest[:end])
}

// newTestBroker creates an isolated broker instance for handler tests.
func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()

	cfg := &config.Config{
		ListenAddr:          ":0",
		DataDir:             t.TempDir(),
		NumPartitions:       3,
		ReplicationFactor:   3,
		MinInSyncReplicas:   2,
		ReplicaMaxLag:       0,
		ReplicaLagTimeoutMs: 10_000,
		AckAllTimeoutMs:     100,
	}

	return broker.NewBroker(cfg)
}

// TestHandleRequestUnknownCommand verifies unsupported commands are rejected.
func TestHandleRequestUnknownCommand(t *testing.T) {
	b := newTestBroker(t)
	req := &protocol.Request{Version: "V1", CorrelationID: "1", Command: "NOPE", Args: []string{}}

	resp := handleRequest(req, b)
	if !strings.Contains(resp, "|ERR|UNKNOWN_COMMAND|") {
		t.Fatalf("unexpected response: %s", resp)
	}
}

// TestHandleProduceValidation checks required produce arguments.
func TestHandleProduceValidation(t *testing.T) {
	b := newTestBroker(t)

	req := &protocol.Request{Version: "V1", CorrelationID: "2", Command: "PRODUCE", Args: []string{"orders"}}
	resp := handleRequest(req, b)
	if !strings.Contains(resp, "|ERR|BAD_REQUEST|") {
		t.Fatalf("expected BAD_REQUEST, got: %s", resp)
	}
}

// TestHandleFlowProduceConsumeOffsetCommit validates core command flow together.
func TestHandleFlowProduceConsumeOffsetCommit(t *testing.T) {
	b := newTestBroker(t)

	produce := &protocol.Request{Version: "V1", CorrelationID: "3", Command: "PRODUCE", Args: []string{"orders", "user1:created"}}
	produceResp := handleRequest(produce, b)
	if !strings.Contains(produceResp, "|OK|partition=") {
		t.Fatalf("unexpected produce response: %s", produceResp)
	}

	consumeBad := &protocol.Request{Version: "V1", CorrelationID: "4", Command: "CONSUME", Args: []string{"orders", "bad", "0"}}
	consumeBadResp := handleRequest(consumeBad, b)
	if !strings.Contains(consumeBadResp, "|ERR|BAD_REQUEST|") {
		t.Fatalf("expected consume parse error, got: %s", consumeBadResp)
	}

	join := &protocol.Request{Version: "V1", CorrelationID: "5", Command: "JOIN", Args: []string{"g1", "orders", "c1"}}
	joinResp := handleRequest(join, b)
	if !strings.Contains(joinResp, "|OK|assigned=") {
		t.Fatalf("unexpected join response: %s", joinResp)
	}

	commit := &protocol.Request{Version: "V1", CorrelationID: "6", Command: "COMMIT", Args: []string{"g1", "orders", "0", "1"}}
	commitResp := handleRequest(commit, b)
	if !strings.Contains(commitResp, "|OK|committed=true") {
		t.Fatalf("unexpected commit response: %s", commitResp)
	}

	offset := &protocol.Request{Version: "V1", CorrelationID: "7", Command: "OFFSET", Args: []string{"g1", "orders", "0"}}
	offsetResp := handleRequest(offset, b)
	if !strings.Contains(offsetResp, "|OK|offset=1") {
		t.Fatalf("unexpected offset response: %s", offsetResp)
	}
}

func TestProduceAcksAllTimeoutThenSucceedsAfterReplicaFetch(t *testing.T) {
	b := newTestBroker(t)

	produce := &protocol.Request{
		Version:       "V1",
		CorrelationID: "8",
		Command:       "PRODUCE",
		Args:          []string{"orders", "user9:created", "acks=all"},
	}
	resp := handleRequest(produce, b)
	if !strings.Contains(resp, "|ERR|REPLICATION_TIMEOUT|") {
		t.Fatalf("expected replication timeout, got: %s", resp)
	}

	// Append with acks=1 first so we have an offset to replicate.
	produceAcks1 := &protocol.Request{
		Version:       "V1",
		CorrelationID: "9",
		Command:       "PRODUCE",
		Args:          []string{"orders", "user9:updated", "acks=1"},
	}
	resp1 := handleRequest(produceAcks1, b)
	if !strings.Contains(resp1, "|OK|partition=") {
		t.Fatalf("expected produce success, got: %s", resp1)
	}
	partition, err := parsePartitionFromProduceResponse(resp1)
	if err != nil {
		t.Fatalf("parse partition: %v, response=%s", err, resp1)
	}

	// Simulate follower replication progress to satisfy ISR.
	replFetch := &protocol.Request{
		Version:       "V1",
		CorrelationID: "10",
		Command:       "REPLICA_FETCH",
		Args:          []string{"orders", strconv.Itoa(partition), "1", "1"},
	}
	replResp := handleRequest(replFetch, b)
	if !strings.Contains(replResp, "|OK|replica=1") {
		t.Fatalf("expected replica fetch ack, got: %s", replResp)
	}

	produceAll := &protocol.Request{
		Version:       "V1",
		CorrelationID: "11",
		Command:       "PRODUCE",
		Args:          []string{"orders", "user9:confirmed", "acks=all"},
	}
	// Ack replica again in goroutine so latest offset is committed.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = handleRequest(&protocol.Request{
			Version:       "V1",
			CorrelationID: "12",
			Command:       "REPLICA_FETCH",
			Args:          []string{"orders", strconv.Itoa(partition), "1", "2"},
		}, b)
	}()

	respAll := handleRequest(produceAll, b)
	if !strings.Contains(respAll, "|OK|partition=") {
		t.Fatalf("expected acks=all success, got: %s", respAll)
	}
}
