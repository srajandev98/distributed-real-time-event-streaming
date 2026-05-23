package coordinator

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"real-time-event-streaming/internal/logging"
)

const (
	assignorRoundRobin = "round_robin"
	assignorRange      = "range"
)

// JoinResult carries generation and ownership after a successful join.
type JoinResult struct {
	Generation int
	Assigned   []int
}

// GroupManager tracks group members and computes partition ownership.
type GroupManager struct {
	numPartitions  int
	sessionTimeout time.Duration
	groups         map[string]*ConsumerGroup
	mutex          sync.Mutex
}

// ConsumerGroup stores in-memory state for one group-topic pair.
type ConsumerGroup struct {
	name        string
	topic       string
	generation  int
	assignor    string
	members     map[string]*GroupMember
	memberOrder []string
	assignments map[string][]int
}

// GroupMember stores per-consumer liveness metadata.
type GroupMember struct {
	consumerID    string
	lastHeartbeat time.Time
}

// NewGroupManager creates a manager with configured partition count.
func NewGroupManager(numPartitions int) *GroupManager {
	return &GroupManager{
		numPartitions:  numPartitions,
		sessionTimeout: 10 * time.Second,
		groups:         make(map[string]*ConsumerGroup),
	}
}

// JoinGroup adds a consumer, bumps generation if needed, and returns assignment.
func (gm *GroupManager) JoinGroup(groupName string, topic string, consumerID string, assignor string) (JoinResult, error) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	now := time.Now()
	group := gm.ensureGroupLocked(groupName, topic, assignor)
	gm.cleanupExpiredLocked(group, now)

	if _, exists := group.members[consumerID]; !exists {
		group.memberOrder = append(group.memberOrder, consumerID)
		group.members[consumerID] = &GroupMember{consumerID: consumerID, lastHeartbeat: now}
		group.generation++
		gm.rebalanceLocked(group)
	} else {
		group.members[consumerID].lastHeartbeat = now
	}

	return JoinResult{Generation: group.generation, Assigned: append([]int(nil), group.assignments[consumerID]...)}, nil
}

// SyncGroup validates generation and returns latest assignment for this member.
func (gm *GroupManager) SyncGroup(groupName string, topic string, consumerID string, generation int) (JoinResult, error) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	group, ok := gm.groups[groupName]
	if !ok || group.topic != topic {
		return JoinResult{}, fmt.Errorf("group not found")
	}
	gm.cleanupExpiredLocked(group, time.Now())

	if generation != group.generation {
		return JoinResult{}, fmt.Errorf("stale generation")
	}
	if _, ok := group.members[consumerID]; !ok {
		return JoinResult{}, fmt.Errorf("unknown member")
	}
	group.members[consumerID].lastHeartbeat = time.Now()

	return JoinResult{Generation: group.generation, Assigned: append([]int(nil), group.assignments[consumerID]...)}, nil
}

// Heartbeat refreshes member liveness and validates generation.
func (gm *GroupManager) Heartbeat(groupName string, topic string, consumerID string, generation int) error {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	group, ok := gm.groups[groupName]
	if !ok || group.topic != topic {
		return fmt.Errorf("group not found")
	}
	gm.cleanupExpiredLocked(group, time.Now())

	if generation != group.generation {
		return fmt.Errorf("stale generation")
	}
	member, ok := group.members[consumerID]
	if !ok {
		return fmt.Errorf("unknown member")
	}
	member.lastHeartbeat = time.Now()
	return nil
}

// ValidateCommit ensures commit writer belongs to current generation and owns partition.
func (gm *GroupManager) ValidateCommit(groupName string, topic string, consumerID string, generation int, partition int) error {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	group, ok := gm.groups[groupName]
	if !ok || group.topic != topic {
		return fmt.Errorf("group not found")
	}
	gm.cleanupExpiredLocked(group, time.Now())

	if generation != group.generation {
		return fmt.Errorf("stale generation")
	}
	if _, ok := group.members[consumerID]; !ok {
		return fmt.Errorf("unknown member")
	}

	for _, p := range group.assignments[consumerID] {
		if p == partition {
			return nil
		}
	}
	return fmt.Errorf("partition not assigned to member")
}

func (gm *GroupManager) ensureGroupLocked(groupName string, topic string, assignor string) *ConsumerGroup {
	group, exists := gm.groups[groupName]
	if !exists {
		if assignor == "" {
			assignor = assignorRoundRobin
		}
		group = &ConsumerGroup{
			name:        groupName,
			topic:       topic,
			generation:  1,
			assignor:    assignor,
			members:     make(map[string]*GroupMember),
			memberOrder: []string{},
			assignments: make(map[string][]int),
		}
		gm.groups[groupName] = group
		return group
	}

	if group.topic != topic {
		group.topic = topic
		group.members = make(map[string]*GroupMember)
		group.memberOrder = []string{}
		group.assignments = make(map[string][]int)
		group.generation++
	}

	if assignor != "" && assignor != group.assignor {
		group.assignor = assignor
		group.generation++
		gm.rebalanceLocked(group)
	}
	return group
}

func (gm *GroupManager) cleanupExpiredLocked(group *ConsumerGroup, now time.Time) {
	active := make([]string, 0, len(group.memberOrder))
	removed := false
	for _, memberID := range group.memberOrder {
		member := group.members[memberID]
		if member == nil {
			removed = true
			continue
		}
		if now.Sub(member.lastHeartbeat) > gm.sessionTimeout {
			delete(group.members, memberID)
			delete(group.assignments, memberID)
			removed = true
			continue
		}
		active = append(active, memberID)
	}
	group.memberOrder = active

	if removed {
		group.generation++
		gm.rebalanceLocked(group)
	}
}

func (gm *GroupManager) rebalanceLocked(group *ConsumerGroup) {
	group.assignments = make(map[string][]int)
	if len(group.memberOrder) == 0 {
		return
	}

	members := append([]string(nil), group.memberOrder...)
	sort.Strings(members)

	switch group.assignor {
	case assignorRange:
		gm.assignRangeLocked(group, members)
	default:
		gm.assignRoundRobinLocked(group, members)
	}

	logging.Info("group rebalance complete", "group", group.name, "topic", group.topic, "generation", group.generation, "assignor", group.assignor, "members", len(group.memberOrder), "assignments", group.assignments)
}

func (gm *GroupManager) assignRoundRobinLocked(group *ConsumerGroup, members []string) {
	for partition := 0; partition < gm.numPartitions; partition++ {
		memberID := members[partition%len(members)]
		group.assignments[memberID] = append(group.assignments[memberID], partition)
	}
}

func (gm *GroupManager) assignRangeLocked(group *ConsumerGroup, members []string) {
	base := gm.numPartitions / len(members)
	extra := gm.numPartitions % len(members)
	start := 0

	for idx, memberID := range members {
		count := base
		if idx < extra {
			count++
		}
		if count == 0 {
			group.assignments[memberID] = []int{}
			continue
		}
		parts := make([]int, 0, count)
		for p := start; p < start+count; p++ {
			parts = append(parts, p)
		}
		group.assignments[memberID] = parts
		start += count
	}
}
