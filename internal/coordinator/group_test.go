package coordinator

import "testing"

func TestJoinGroupSingleConsumerGetsAllPartitions(t *testing.T) {
	gm := NewGroupManager(3)

	assigned := gm.JoinGroup("g1", "orders", "c1")
	if len(assigned) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(assigned))
	}
	if assigned[0] != 0 || assigned[1] != 1 || assigned[2] != 2 {
		t.Fatalf("unexpected assignments: %v", assigned)
	}
}

func TestJoinGroupRebalanceTwoConsumers(t *testing.T) {
	gm := NewGroupManager(3)

	_ = gm.JoinGroup("g1", "orders", "c1")
	assignedC2 := gm.JoinGroup("g1", "orders", "c2")

	if len(assignedC2) != 1 || assignedC2[0] != 1 {
		t.Fatalf("unexpected assignments for c2: %v", assignedC2)
	}

	assignedC1 := gm.groups["g1"].assignments["c1"]
	if len(assignedC1) != 2 || assignedC1[0] != 0 || assignedC1[1] != 2 {
		t.Fatalf("unexpected assignments for c1: %v", assignedC1)
	}
}
