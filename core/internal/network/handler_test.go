package network

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"flux/internal/broker"
	"flux/internal/config"
	"flux/internal/protocol"
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
	if !strings.Contains(joinResp, "|OK|generation=") || !strings.Contains(joinResp, "assigned=") {
		t.Fatalf("unexpected join response: %s", joinResp)
	}

	commit := &protocol.Request{Version: "V1", CorrelationID: "6", Command: "COMMIT", Args: []string{"g1", "orders", "c1", "2", "0", "1"}}
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

func TestHeartbeatAndSyncCommands(t *testing.T) {
	b := newTestBroker(t)

	join := &protocol.Request{Version: "V1", CorrelationID: "20", Command: "JOIN", Args: []string{"g1", "orders", "c1"}}
	joinResp := handleRequest(join, b)
	if !strings.Contains(joinResp, "|OK|generation=2") {
		t.Fatalf("unexpected join response: %s", joinResp)
	}

	hb := &protocol.Request{Version: "V1", CorrelationID: "21", Command: "HEARTBEAT", Args: []string{"g1", "orders", "c1", "2"}}
	hbResp := handleRequest(hb, b)
	if !strings.Contains(hbResp, "|OK|heartbeat=ok") {
		t.Fatalf("unexpected heartbeat response: %s", hbResp)
	}

	sync := &protocol.Request{Version: "V1", CorrelationID: "22", Command: "SYNC", Args: []string{"g1", "orders", "c1", "2"}}
	syncResp := handleRequest(sync, b)
	if !strings.Contains(syncResp, "|OK|generation=2") {
		t.Fatalf("unexpected sync response: %s", syncResp)
	}

	staleHB := &protocol.Request{Version: "V1", CorrelationID: "23", Command: "HEARTBEAT", Args: []string{"g1", "orders", "c1", "1"}}
	staleHBResp := handleRequest(staleHB, b)
	if !strings.Contains(staleHBResp, "|ERR|GENERATION_MISMATCH|") {
		t.Fatalf("expected generation mismatch, got: %s", staleHBResp)
	}
}

func TestLeaveCommand(t *testing.T) {
	b := newTestBroker(t)

	join := &protocol.Request{Version: "V1", CorrelationID: "30", Command: "JOIN", Args: []string{"g1", "orders", "c1"}}
	joinResp := handleRequest(join, b)
	if !strings.Contains(joinResp, "|OK|generation=2") {
		t.Fatalf("unexpected join response: %s", joinResp)
	}

	leave := &protocol.Request{Version: "V1", CorrelationID: "31", Command: "LEAVE", Args: []string{"g1", "orders", "c1", "2"}}
	leaveResp := handleRequest(leave, b)
	if !strings.Contains(leaveResp, "|OK|left=true") {
		t.Fatalf("unexpected leave response: %s", leaveResp)
	}

	staleCommit := &protocol.Request{
		Version:       "V1",
		CorrelationID: "32",
		Command:       "COMMIT",
		Args:          []string{"g1", "orders", "c1", "2", "0", "1"},
	}
	staleCommitResp := handleRequest(staleCommit, b)
	if !strings.Contains(staleCommitResp, "|ERR|GENERATION_MISMATCH|") {
		t.Fatalf("expected stale generation after leave, got: %s", staleCommitResp)
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

func TestSetPartitionRoleBlocksAndRestoresProduce(t *testing.T) {
	b := newTestBroker(t)
	key := "user-role"
	partition := b.Storage.PartitionForKey(key)

	setFollower := &protocol.Request{
		Version:       "V1",
		CorrelationID: "13",
		Command:       "SET_PARTITION_ROLE",
		Args:          []string{"orders", strconv.Itoa(partition), "follower"},
	}
	setFollowerResp := handleRequest(setFollower, b)
	if !strings.Contains(setFollowerResp, "|OK|topic=orders") {
		t.Fatalf("expected role update success, got: %s", setFollowerResp)
	}

	produceWhileFollower := &protocol.Request{
		Version:       "V1",
		CorrelationID: "14",
		Command:       "PRODUCE",
		Args:          []string{"orders", key + ":created"},
	}
	produceFollowerResp := handleRequest(produceWhileFollower, b)
	if !strings.Contains(produceFollowerResp, "|ERR|NOT_LEADER|") {
		t.Fatalf("expected NOT_LEADER while follower, got: %s", produceFollowerResp)
	}

	setLeader := &protocol.Request{
		Version:       "V1",
		CorrelationID: "15",
		Command:       "SET_PARTITION_ROLE",
		Args:          []string{"orders", strconv.Itoa(partition), "leader"},
	}
	setLeaderResp := handleRequest(setLeader, b)
	if !strings.Contains(setLeaderResp, "|OK|topic=orders") {
		t.Fatalf("expected role update to leader success, got: %s", setLeaderResp)
	}

	produceAsLeader := &protocol.Request{
		Version:       "V1",
		CorrelationID: "16",
		Command:       "PRODUCE",
		Args:          []string{"orders", key + ":created"},
	}
	produceLeaderResp := handleRequest(produceAsLeader, b)
	if !strings.Contains(produceLeaderResp, "|OK|partition=") {
		t.Fatalf("expected produce success after leader transition, got: %s", produceLeaderResp)
	}
}

func TestAdminControlPlaneFlow(t *testing.T) {
	b := newTestBroker(t)

	createTopic := &protocol.Request{
		Version:       "V1",
		CorrelationID: "40",
		Command:       "ADMIN_CREATE_TOPIC",
		Args:          []string{"payments", "2", "2"},
	}
	createTopicResp := handleRequest(createTopic, b)
	if !strings.Contains(createTopicResp, "|OK|topic=payments") {
		t.Fatalf("unexpected create topic response: %s", createTopicResp)
	}

	registerBroker0 := &protocol.Request{
		Version:       "V1",
		CorrelationID: "41",
		Command:       "ADMIN_REGISTER_BROKER",
		Args:          []string{"0", "127.0.0.1", "9092"},
	}
	registerBrokerResp0 := handleRequest(registerBroker0, b)
	if !strings.Contains(registerBrokerResp0, "|OK|broker_id=0") {
		t.Fatalf("unexpected register broker0 response: %s", registerBrokerResp0)
	}

	registerBroker1 := &protocol.Request{
		Version:       "V1",
		CorrelationID: "42",
		Command:       "ADMIN_REGISTER_BROKER",
		Args:          []string{"1", "127.0.0.1", "9093", "100"},
	}
	registerBrokerResp1 := handleRequest(registerBroker1, b)
	if !strings.Contains(registerBrokerResp1, "|OK|broker_id=1") {
		t.Fatalf("unexpected register broker1 response: %s", registerBrokerResp1)
	}

	setLeader := &protocol.Request{
		Version:       "V1",
		CorrelationID: "43",
		Command:       "ADMIN_SET_PARTITION_LEADER",
		Args:          []string{"payments", "1", "1", "1,0"},
	}
	setLeaderResp := handleRequest(setLeader, b)
	if !strings.Contains(setLeaderResp, "|OK|topic=payments partition=1 leader=1") {
		t.Fatalf("unexpected set leader response: %s", setLeaderResp)
	}

	heartbeat := &protocol.Request{
		Version:       "V1",
		CorrelationID: "44",
		Command:       "ADMIN_BROKER_HEARTBEAT",
		Args:          []string{"1"},
	}
	heartbeatResp := handleRequest(heartbeat, b)
	if !strings.Contains(heartbeatResp, "|OK|broker_id=1 heartbeat=ok") {
		t.Fatalf("unexpected heartbeat response: %s", heartbeatResp)
	}

	metadata := &protocol.Request{
		Version:       "V1",
		CorrelationID: "45",
		Command:       "ADMIN_GET_METADATA",
		Args:          []string{},
	}
	metadataResp := handleRequest(metadata, b)
	if !strings.Contains(metadataResp, "|OK|topics=[payments]") {
		t.Fatalf("unexpected metadata response: %s", metadataResp)
	}
	if !strings.Contains(metadataResp, "brokers=[0 1]") {
		t.Fatalf("metadata response missing brokers: %s", metadataResp)
	}
}

func TestAdminValidationErrors(t *testing.T) {
	b := newTestBroker(t)

	createBad := &protocol.Request{
		Version:       "V1",
		CorrelationID: "50",
		Command:       "ADMIN_CREATE_TOPIC",
		Args:          []string{"payments", "x", "2"},
	}
	if resp := handleRequest(createBad, b); !strings.Contains(resp, "|ERR|BAD_REQUEST|") {
		t.Fatalf("expected BAD_REQUEST for invalid partition count, got: %s", resp)
	}

	setLeaderBad := &protocol.Request{
		Version:       "V1",
		CorrelationID: "51",
		Command:       "ADMIN_SET_PARTITION_LEADER",
		Args:          []string{"payments", "0", "1", "bad,isr"},
	}
	if resp := handleRequest(setLeaderBad, b); !strings.Contains(resp, "|ERR|BAD_REQUEST|") {
		t.Fatalf("expected BAD_REQUEST for invalid isr list, got: %s", resp)
	}
}

func TestProduceMigratesTopicMetadataToController(t *testing.T) {
	b := newTestBroker(t)

	resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "60",
		Command:       "PRODUCE",
		Args:          []string{"invoices", "u1:created"},
	}, b)
	if !strings.Contains(resp, "|OK|partition=") {
		t.Fatalf("expected produce success, got: %s", resp)
	}

	snapshot := b.Controller.Snapshot()
	topic, ok := snapshot.Topics["invoices"]
	if !ok {
		t.Fatalf("expected controller metadata to include produced topic")
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions in migrated metadata, got: %d", len(topic.Partitions))
	}
	for partitionID, partition := range topic.Partitions {
		if partition.LeaderID != b.LocalBrokerID {
			t.Fatalf("expected local broker to lead partition %d, got leader=%d", partitionID, partition.LeaderID)
		}
	}
}

func TestAdminFencingHealthAndElectionFlow(t *testing.T) {
	b := newTestBroker(t)

	createTopic := &protocol.Request{
		Version:       "V1",
		CorrelationID: "70",
		Command:       "ADMIN_CREATE_TOPIC",
		Args:          []string{"election", "2", "2"},
	}
	if resp := handleRequest(createTopic, b); !strings.Contains(resp, "|OK|topic=election") {
		t.Fatalf("unexpected create topic response: %s", resp)
	}
	if resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "71",
		Command:       "ADMIN_REGISTER_BROKER",
		Args:          []string{"1", "127.0.0.1", "9093"},
	}, b); !strings.Contains(resp, "|OK|broker_id=1") {
		t.Fatalf("unexpected register broker response: %s", resp)
	}
	if resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "72",
		Command:       "ADMIN_SET_PARTITION_LEADER",
		Args:          []string{"election", "0", "0", "0,1"},
	}, b); !strings.Contains(resp, "|OK|topic=election partition=0 leader=0") {
		t.Fatalf("unexpected set leader response: %s", resp)
	}

	if resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "73",
		Command:       "ADMIN_FENCE_BROKER",
		Args:          []string{"0"},
	}, b); !strings.Contains(resp, "|OK|broker_id=0 fenced=true") {
		t.Fatalf("unexpected fence response: %s", resp)
	}

	if resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "74",
		Command:       "ADMIN_ELECT_TOPIC_LEADERS",
		Args:          []string{"election"},
	}, b); !strings.Contains(resp, "|OK|topic=election elected_changes=") {
		t.Fatalf("unexpected elect response: %s", resp)
	}

	snapshot := b.Controller.Snapshot()
	if snapshot.Topics["election"].Partitions[0].LeaderID != 1 {
		t.Fatalf("expected leader failover to broker 1, got=%d", snapshot.Topics["election"].Partitions[0].LeaderID)
	}

	// Reconcile with an aggressive timeout should fence stale brokers.
	if resp := handleRequest(&protocol.Request{
		Version:       "V1",
		CorrelationID: "75",
		Command:       "ADMIN_RECONCILE_BROKER_HEALTH",
		Args:          []string{"1"},
	}, b); !strings.Contains(resp, "|OK|reconciled=") {
		t.Fatalf("unexpected reconcile response: %s", resp)
	}
}
