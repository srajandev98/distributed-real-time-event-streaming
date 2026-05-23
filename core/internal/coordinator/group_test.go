package coordinator

import (
	"testing"
	"time"
)

func TestJoinGroupSingleConsumerGetsAllPartitions(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())

	result, err := gm.JoinGroup("g1", "orders", "c1", "")
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if result.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", result.Generation)
	}
	if len(result.Assigned) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(result.Assigned))
	}
}

func TestJoinGroupRebalanceTwoConsumersRoundRobin(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())
	_, _ = gm.JoinGroup("g1", "orders", "c1", "round_robin")
	assignedC2, err := gm.JoinGroup("g1", "orders", "c2", "round_robin")
	if err != nil {
		t.Fatalf("join c2 failed: %v", err)
	}

	if len(assignedC2.Assigned) != 1 || assignedC2.Assigned[0] != 1 {
		t.Fatalf("unexpected assignments for c2: %v", assignedC2.Assigned)
	}

	assignedC1 := gm.groups["g1"].assignments["c1"]
	if len(assignedC1) != 2 || assignedC1[0] != 0 || assignedC1[1] != 2 {
		t.Fatalf("unexpected assignments for c1: %v", assignedC1)
	}
}

func TestRangeAssignor(t *testing.T) {
	gm := NewGroupManager(5, t.TempDir())
	_, _ = gm.JoinGroup("g1", "orders", "c1", "range")
	c2, err := gm.JoinGroup("g1", "orders", "c2", "range")
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	c1 := gm.groups["g1"].assignments["c1"]

	if len(c1) != 3 || c1[0] != 0 || c1[2] != 2 {
		t.Fatalf("unexpected c1 range assignments: %v", c1)
	}
	if len(c2.Assigned) != 2 || c2.Assigned[0] != 3 || c2.Assigned[1] != 4 {
		t.Fatalf("unexpected c2 range assignments: %v", c2.Assigned)
	}
}

func TestHeartbeatAndSyncValidateGeneration(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())
	joined, _ := gm.JoinGroup("g1", "orders", "c1", "")

	if err := gm.Heartbeat("g1", "orders", "c1", joined.Generation); err != nil {
		t.Fatalf("heartbeat should succeed: %v", err)
	}
	if err := gm.Heartbeat("g1", "orders", "c1", joined.Generation-1); err == nil {
		t.Fatalf("expected stale generation heartbeat failure")
	}

	if _, err := gm.SyncGroup("g1", "orders", "c1", joined.Generation); err != nil {
		t.Fatalf("sync should succeed: %v", err)
	}
	if _, err := gm.SyncGroup("g1", "orders", "c1", joined.Generation-1); err == nil {
		t.Fatalf("expected stale generation sync failure")
	}
}

func TestSessionTimeoutEvictsMemberAndBumpsGeneration(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())
	gm.sessionTimeout = 30 * time.Millisecond

	first, _ := gm.JoinGroup("g1", "orders", "c1", "")
	time.Sleep(40 * time.Millisecond)
	second, _ := gm.JoinGroup("g1", "orders", "c2", "")

	if second.Generation <= first.Generation {
		t.Fatalf("expected generation bump after timeout; first=%d second=%d", first.Generation, second.Generation)
	}
	if _, exists := gm.groups["g1"].members["c1"]; exists {
		t.Fatalf("expected c1 to be evicted after session timeout")
	}
}

func TestValidateCommitRequiresOwnership(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())
	joined, _ := gm.JoinGroup("g1", "orders", "c1", "")

	if err := gm.ValidateCommit("g1", "orders", "c1", joined.Generation, 0); err != nil {
		t.Fatalf("expected commit validation success: %v", err)
	}
	if err := gm.ValidateCommit("g1", "orders", "c1", joined.Generation-1, 0); err == nil {
		t.Fatalf("expected stale generation failure")
	}
	if err := gm.ValidateCommit("g1", "orders", "c1", joined.Generation, 9); err == nil {
		t.Fatalf("expected unassigned partition failure")
	}
}

func TestLeaveGroupRemovesMemberAndBumpsGeneration(t *testing.T) {
	gm := NewGroupManager(3, t.TempDir())
	joined, _ := gm.JoinGroup("g1", "orders", "c1", "")
	if err := gm.LeaveGroup("g1", "orders", "c1", joined.Generation); err != nil {
		t.Fatalf("leave should succeed: %v", err)
	}
	if _, exists := gm.groups["g1"].members["c1"]; exists {
		t.Fatalf("expected member removed after leave")
	}
}

func TestGroupStatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	gm := NewGroupManager(3, dir)
	joined, err := gm.JoinGroup("g1", "orders", "c1", "round_robin")
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if joined.Generation < 2 {
		t.Fatalf("expected generation to increment, got %d", joined.Generation)
	}

	reloaded := NewGroupManager(3, dir)
	group := reloaded.groups["g1"]
	if group == nil {
		t.Fatalf("expected group to reload from disk")
	}
	if group.generation != joined.Generation {
		t.Fatalf("expected generation %d, got %d", joined.Generation, group.generation)
	}
	if group.assignor != "round_robin" {
		t.Fatalf("unexpected assignor: %s", group.assignor)
	}
	if len(group.memberOrder) != 1 || group.memberOrder[0] != "c1" {
		t.Fatalf("unexpected member order after reload: %v", group.memberOrder)
	}
}
