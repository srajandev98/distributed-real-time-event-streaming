package controlplane

import (
	"fmt"
	"sort"
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
	if leaderID < -1 {
		return ApplyResult{}, fmt.Errorf("leader id must be >= -1")
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

func (c *Controller) SetBrokerFence(brokerID int, fenced bool) (ApplyResult, error) {
	if brokerID < 0 {
		return ApplyResult{}, fmt.Errorf("broker id must be >= 0")
	}
	return c.store.Apply(Command{
		Type: CommandSetBrokerFence,
		BrokerFence: &BrokerFenceUpdate{
			BrokerID: brokerID,
			Fenced:   fenced,
		},
	})
}

func (c *Controller) ReconcileBrokerHealth(timeout time.Duration) (int, error) {
	if timeout <= 0 {
		return 0, fmt.Errorf("timeout must be > 0")
	}
	now := time.Now().UTC()
	snapshot := c.store.Snapshot()
	ids := make([]int, 0, len(snapshot.Brokers))
	for id := range snapshot.Brokers {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	changes := 0
	for _, id := range ids {
		b := snapshot.Brokers[id]
		stale := b.LastHeartbeat.IsZero() || now.Sub(b.LastHeartbeat) > timeout
		targetFenced := stale
		if b.Fenced != targetFenced {
			if _, err := c.SetBrokerFence(id, targetFenced); err != nil {
				return changes, err
			}
			changes++
		}
	}
	return changes, nil
}

func (c *Controller) ElectTopicLeaders(topic string) (int, error) {
	if topic == "" {
		return 0, fmt.Errorf("topic is required")
	}
	snapshot := c.store.Snapshot()
	topicMeta, ok := snapshot.Topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	partitionIDs := make([]int, 0, len(topicMeta.Partitions))
	for pid := range topicMeta.Partitions {
		partitionIDs = append(partitionIDs, pid)
	}
	sort.Ints(partitionIDs)

	changes := 0
	for _, pid := range partitionIDs {
		partition := topicMeta.Partitions[pid]
		targetLeader := partition.LeaderID

		if !isBrokerEligible(snapshot.Brokers, targetLeader) {
			targetLeader = electFromReplicas(snapshot.Brokers, partition.Replicas)
		}
		if targetLeader != partition.LeaderID {
			isr := selectEligibleReplicas(snapshot.Brokers, partition.Replicas)
			if len(isr) == 0 && targetLeader >= 0 {
				isr = []int{targetLeader}
			}
			if _, err := c.SetPartitionLeader(topic, pid, targetLeader, isr); err != nil {
				return changes, err
			}
			changes++
		}
	}
	return changes, nil
}

func isBrokerEligible(brokers map[int]BrokerMetadata, brokerID int) bool {
	broker, ok := brokers[brokerID]
	if !ok {
		return false
	}
	return !broker.Fenced
}

func electFromReplicas(brokers map[int]BrokerMetadata, replicas []int) int {
	for _, replicaID := range replicas {
		if isBrokerEligible(brokers, replicaID) {
			return replicaID
		}
	}
	return -1
}

func selectEligibleReplicas(brokers map[int]BrokerMetadata, replicas []int) []int {
	out := make([]int, 0, len(replicas))
	for _, replicaID := range replicas {
		if isBrokerEligible(brokers, replicaID) {
			out = append(out, replicaID)
		}
	}
	return out
}

func (c *Controller) Snapshot() MetadataSnapshot {
	return c.store.Snapshot()
}
