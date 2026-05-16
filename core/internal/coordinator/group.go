package coordinator

import (
	"sync"

	"real-time-event-streaming/internal/logging"
)

// GroupManager tracks group members and computes partition ownership.
type GroupManager struct {
	numPartitions int
	groups        map[string]*ConsumerGroup
	mutex         sync.Mutex
}

// ConsumerGroup stores in-memory state for one group-topic pair.
type ConsumerGroup struct {
	name        string
	topic       string
	consumers   []string
	assignments map[string][]int
}

// NewGroupManager creates a manager with configured partition count.
func NewGroupManager(numPartitions int) *GroupManager {
	return &GroupManager{
		numPartitions: numPartitions,
		groups:        make(map[string]*ConsumerGroup),
	}
}

// JoinGroup adds a consumer and returns its assigned partitions.
func (gm *GroupManager) JoinGroup(groupName string, topic string, consumerID string) []int {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	group, exists := gm.groups[groupName]
	if !exists {
		group = &ConsumerGroup{
			name:        groupName,
			topic:       topic,
			consumers:   []string{},
			assignments: make(map[string][]int),
		}
		gm.groups[groupName] = group
	}

	group.consumers = append(group.consumers, consumerID)
	gm.rebalance(group)
	return group.assignments[consumerID]
}

// rebalance applies simple round-robin partition assignment.
func (gm *GroupManager) rebalance(group *ConsumerGroup) {
	group.assignments = make(map[string][]int)

	for partition := 0; partition < gm.numPartitions; partition++ {
		consumerIndex := partition % len(group.consumers)
		consumerID := group.consumers[consumerIndex]
		group.assignments[consumerID] = append(group.assignments[consumerID], partition)
	}

	logging.Info("group rebalance complete", "group", group.name, "topic", group.topic, "members", len(group.consumers), "assignments", group.assignments)
}
