package coordinator

import (
	"fmt"
	"sync"
)

const NumPartitions = 3

type GroupManager struct {
	groups map[string]*ConsumerGroup
	mutex  sync.Mutex
}

type ConsumerGroup struct {
	name        string
	topic       string
	consumers   []string
	assignments map[string][]int
}

func NewGroupManager() *GroupManager {

	return &GroupManager{
		groups: make(map[string]*ConsumerGroup),
	}
}

func (gm *GroupManager) JoinGroup(
	groupName string,
	topic string,
	consumerID string,
) []int {

	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	group, exists := gm.groups[groupName]

	if !exists {

		group = &ConsumerGroup{
			name:      groupName,
			topic:     topic,
			consumers: []string{},
			assignments: make(
				map[string][]int,
			),
		}

		gm.groups[groupName] = group
	}

	group.consumers = append(
		group.consumers,
		consumerID,
	)

	gm.rebalance(group)

	return group.assignments[consumerID]
}

func (gm *GroupManager) rebalance(
	group *ConsumerGroup,
) {

	group.assignments = make(
		map[string][]int,
	)

	for partition := 0; partition < NumPartitions; partition++ {

		consumerIndex := partition %
			len(group.consumers)

		consumerID := group.consumers[consumerIndex]

		group.assignments[consumerID] =
			append(
				group.assignments[consumerID],
				partition,
			)
	}

	fmt.Println("Rebalance complete")

	for consumer, partitions := range group.assignments {

		fmt.Printf(
			"consumer=%s partitions=%v\n",
			consumer,
			partitions,
		)
	}
}
