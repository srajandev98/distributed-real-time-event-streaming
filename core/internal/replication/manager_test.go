package replication

import (
	"testing"
	"time"
)

func newTestManager() *Manager {
	return NewManager(Config{
		ReplicationFactor: 3,
		MinInSyncReplicas: 2,
		MaxReplicaLag:     0,
		ReplicaLagTimeout: 5 * time.Second,
		AckAllTimeout:     250 * time.Millisecond,
	}, nil)
}

func TestHighWatermarkAdvancesWithISR(t *testing.T) {
	m := newTestManager()
	m.OnLeaderAppend("orders", 0, 0)

	if got := m.HighWatermark("orders", 0); got != 0 {
		t.Fatalf("expected hw=0 with leader-only ISR, got %d", got)
	}

	if err := m.AckReplica("orders", 0, 1, 0); err != nil {
		t.Fatalf("ack replica 1: %v", err)
	}
	if err := m.AckReplica("orders", 0, 2, 0); err != nil {
		t.Fatalf("ack replica 2: %v", err)
	}

	if got := m.HighWatermark("orders", 0); got != 0 {
		t.Fatalf("expected hw=0 after follower acks, got %d", got)
	}

	status := m.Status("orders", 0)
	if status.UnderReplicated {
		t.Fatalf("expected fully replicated partition")
	}
	if len(status.InSyncReplicas) != 3 {
		t.Fatalf("expected isr size 3, got %d", len(status.InSyncReplicas))
	}
}

func TestAckAllTimeoutWithoutFollowerProgress(t *testing.T) {
	m := newTestManager()
	m.OnLeaderAppend("orders", 0, 1)

	if err := m.WaitForAckAll("orders", 0, 1); err == nil {
		t.Fatalf("expected ack-all timeout when followers are not in-sync")
	}
}

func TestAckAllSucceedsAfterFollowerAcks(t *testing.T) {
	m := newTestManager()
	m.OnLeaderAppend("orders", 0, 2)

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = m.AckReplica("orders", 0, 1, 2)
	}()

	if err := m.WaitForAckAll("orders", 0, 2); err != nil {
		t.Fatalf("expected acks=all success, got: %v", err)
	}
}

func TestRoleTransitions(t *testing.T) {
	m := newTestManager()

	if !m.IsLeader("orders", 0) {
		t.Fatalf("expected default role to be leader")
	}

	if err := m.SetRole("orders", 0, "follower"); err != nil {
		t.Fatalf("set follower role: %v", err)
	}
	if m.IsLeader("orders", 0) {
		t.Fatalf("expected follower role")
	}
	if err := m.AckReplica("orders", 0, 1, 0); err == nil {
		t.Fatalf("expected ack failure when partition is follower")
	}

	if err := m.SetRole("orders", 0, "leader"); err != nil {
		t.Fatalf("set leader role: %v", err)
	}
	if !m.IsLeader("orders", 0) {
		t.Fatalf("expected leader role")
	}

	status := m.Status("orders", 0)
	if status.Role != "leader" {
		t.Fatalf("expected status role leader, got %s", status.Role)
	}
}
