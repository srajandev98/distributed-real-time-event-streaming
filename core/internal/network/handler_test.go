package network

import (
	"strings"
	"testing"

	"real-time-event-streaming/internal/broker"
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/protocol"
)

// newTestBroker creates an isolated broker instance for handler tests.
func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()

	cfg := &config.Config{
		ListenAddr:    ":0",
		DataDir:       t.TempDir(),
		NumPartitions: 3,
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
