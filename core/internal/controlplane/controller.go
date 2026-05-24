package controlplane

import (
	"fmt"
	"time"
)

// Controller owns metadata mutations for cluster topology and leadership.
type Controller struct {
	store MetadataStore
}

func NewController(store MetadataStore) *Controller {
	return &Controller{store: store}
}

func (c *Controller) CreateTopic(name string, partitions int, replicationFactor int) (ApplyResult, error) {
	if name == "" {
		return ApplyResult{}, fmt.Errorf("topic name is required")
	}
	if partitions <= 0 {
		return ApplyResult{}, fmt.Errorf("partitions must be > 0")
	}
	if replicationFactor <= 0 {
		return ApplyResult{}, fmt.Errorf("replication factor must be > 0")
	}

	replicas := make([]int, 0, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		replicas = append(replicas, i)
	}
	partitionMap := make(map[int]PartitionMetadata, partitions)
	for i := 0; i < partitions; i++ {
		partitionMap[i] = PartitionMetadata{
			ID:       i,
			LeaderID: -1,
			Replicas: append([]int(nil), replicas...),
			ISR:      append([]int(nil), replicas...),
		}
	}

	return c.store.Apply(Command{
		Type: CommandUpsertTopic,
		Topic: &TopicMetadata{
			Name:       name,
			Partitions: partitionMap,
			Configs:    map[string]string{},
		},
	})
}

func (c *Controller) RegisterBroker(id int, host string, port int, epoch int64) (ApplyResult, error) {
	if id < 0 {
		return ApplyResult{}, fmt.Errorf("broker id must be >= 0")
	}
	if host == "" {
		return ApplyResult{}, fmt.Errorf("broker host is required")
	}
	if port <= 0 {
		return ApplyResult{}, fmt.Errorf("broker port must be > 0")
	}
	if epoch <= 0 {
		epoch = time.Now().UnixNano()
	}

	return c.store.Apply(Command{
		Type: CommandRegisterBroker,
		Broker: &BrokerMetadata{
			ID:            id,
			Host:          host,
			Port:          port,
			Epoch:         epoch,
			LastHeartbeat: time.Now().UTC(),
		},
	})
}

func (c *Controller) HeartbeatBroker(id int) (ApplyResult, error) {
	return c.store.Apply(Command{
		Type:            CommandBrokerHeartbeat,
		BrokerHeartbeat: &BrokerHeartbeat{BrokerID: id},
	})
}

func (c *Controller) SetPartitionLeader(topic string, partition int, leaderID int, isr []int) (ApplyResult, error) {
	if topic == "" {
		return ApplyResult{}, fmt.Errorf("topic is required")
	}
	if partition < 0 {
		return ApplyResult{}, fmt.Errorf("partition must be >= 0")
	}
	if leaderID < 0 {
		return ApplyResult{}, fmt.Errorf("leader id must be >= 0")
	}
	return c.store.Apply(Command{
		Type: CommandSetPartitionLeader,
		PartitionLeader: &PartitionLeaderUpdate{
			Topic:     topic,
			Partition: partition,
			LeaderID:  leaderID,
			ISR:       append([]int(nil), isr...),
		},
	})
}

func (c *Controller) Snapshot() MetadataSnapshot {
	return c.store.Snapshot()
}
