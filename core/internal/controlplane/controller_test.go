package controlplane

import (
	"testing"
	"time"
)

func TestControllerTopicBrokerAndLeaderFlow(t *testing.T) {
	store := NewRaftMetadataStore()
	controller := NewController(store)

	if _, err := controller.CreateTopic("orders", 3, 2); err != nil {
		t.Fatalf("create topic failed: %v", err)
	}

	if _, err := controller.RegisterBroker(0, "127.0.0.1", 9092, 1); err != nil {
		t.Fatalf("register broker 0 failed: %v", err)
	}
	if _, err := controller.RegisterBroker(1, "127.0.0.1", 9093, 2); err != nil {
		t.Fatalf("register broker 1 failed: %v", err)
	}

	before := controller.Snapshot().Brokers[0].LastHeartbeat
	time.Sleep(2 * time.Millisecond)
	if _, err := controller.HeartbeatBroker(0); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	after := controller.Snapshot().Brokers[0].LastHeartbeat
	if !after.After(before) {
		t.Fatalf("heartbeat did not advance timestamp: before=%v after=%v", before, after)
	}

	if _, err := controller.SetPartitionLeader("orders", 1, 0, []int{0, 1}); err != nil {
		t.Fatalf("set leader failed: %v", err)
	}
	snapshot := controller.Snapshot()
	partition := snapshot.Topics["orders"].Partitions[1]
	if partition.LeaderID != 0 {
		t.Fatalf("unexpected leader id: got=%d want=0", partition.LeaderID)
	}
	if len(partition.ISR) != 2 || partition.ISR[0] != 0 || partition.ISR[1] != 1 {
		t.Fatalf("unexpected isr: %#v", partition.ISR)
	}
}

func TestControllerRejectsInvalidInput(t *testing.T) {
	controller := NewController(NewRaftMetadataStore())
	if _, err := controller.CreateTopic("", 1, 1); err == nil {
		t.Fatal("expected create topic validation error")
	}
	if _, err := controller.RegisterBroker(-1, "127.0.0.1", 9092, 1); err == nil {
		t.Fatal("expected register broker validation error")
	}
	if _, err := controller.HeartbeatBroker(99); err == nil {
		t.Fatal("expected heartbeat error for unknown broker")
	}
}

func TestControllerReconcileHealthAndElection(t *testing.T) {
	store := NewRaftMetadataStore()
	controller := NewController(store)

	if _, err := controller.RegisterBroker(0, "127.0.0.1", 9092, 1); err != nil {
		t.Fatalf("register broker 0 failed: %v", err)
	}
	if _, err := controller.RegisterBroker(1, "127.0.0.1", 9093, 2); err != nil {
		t.Fatalf("register broker 1 failed: %v", err)
	}
	if _, err := controller.CreateTopic("orders", 2, 2); err != nil {
		t.Fatalf("create topic failed: %v", err)
	}
	if _, err := controller.SetPartitionLeader("orders", 0, 0, []int{0, 1}); err != nil {
		t.Fatalf("set leader failed: %v", err)
	}

	// Fence broker 0 and ensure election moves leader to broker 1.
	if _, err := controller.SetBrokerFence(0, true); err != nil {
		t.Fatalf("set broker fence failed: %v", err)
	}
	changes, err := controller.ElectTopicLeaders("orders")
	if err != nil {
		t.Fatalf("elect leaders failed: %v", err)
	}
	if changes == 0 {
		t.Fatalf("expected at least one leader change")
	}
	snapshot := controller.Snapshot()
	if snapshot.Topics["orders"].Partitions[0].LeaderID != 1 {
		t.Fatalf("expected leader failover to broker 1, got=%d", snapshot.Topics["orders"].Partitions[0].LeaderID)
	}

	// Make broker 1 stale and reconcile -> should fence broker 1.
	broker1 := snapshot.Brokers[1]
	broker1.LastHeartbeat = time.Now().Add(-2 * time.Minute)
	if _, err := store.Apply(Command{Type: CommandRegisterBroker, Broker: &broker1}); err != nil {
		t.Fatalf("force broker heartbeat timestamp failed: %v", err)
	}
	reconciled, err := controller.ReconcileBrokerHealth(10 * time.Second)
	if err != nil {
		t.Fatalf("reconcile health failed: %v", err)
	}
	if reconciled == 0 {
		t.Fatalf("expected fenced-state changes")
	}
	snapshot = controller.Snapshot()
	if !snapshot.Brokers[1].Fenced {
		t.Fatalf("expected broker 1 fenced after stale heartbeat")
	}

	// Heartbeat should clear fence status.
	if _, err := controller.HeartbeatBroker(1); err != nil {
		t.Fatalf("heartbeat broker failed: %v", err)
	}
	snapshot = controller.Snapshot()
	if snapshot.Brokers[1].Fenced {
		t.Fatalf("expected heartbeat to clear fenced state")
	}
}
