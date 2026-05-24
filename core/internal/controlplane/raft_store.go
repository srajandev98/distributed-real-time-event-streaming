package controlplane

import (
	"fmt"
	"sync"
	"time"
)

// RaftMetadataStore is an in-memory metadata state machine with Raft-like term/index tracking.
// This is a scaffold for integrating a real Raft transport/log in Phase 4.
type RaftMetadataStore struct {
	mu       sync.Mutex
	term     uint64
	index    uint64
	metadata MetadataSnapshot
}

func NewRaftMetadataStore() *RaftMetadataStore {
	return &RaftMetadataStore{
		term: 1,
		metadata: MetadataSnapshot{
			Topics:  map[string]TopicMetadata{},
			Brokers: map[int]BrokerMetadata{},
		},
	}
}

func (s *RaftMetadataStore) Apply(cmd Command) (ApplyResult, error) {
	if err := validateCommand(cmd); err != nil {
		return ApplyResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case CommandUpsertTopic:
		s.metadata.Topics[cmd.Topic.Name] = cloneTopic(*cmd.Topic)
	case CommandRegisterBroker:
		broker := *cmd.Broker
		if broker.LastHeartbeat.IsZero() {
			broker.LastHeartbeat = time.Now().UTC()
		}
		s.metadata.Brokers[broker.ID] = broker
	case CommandBrokerHeartbeat:
		b, ok := s.metadata.Brokers[cmd.BrokerHeartbeat.BrokerID]
		if !ok {
			return ApplyResult{}, fmt.Errorf("broker not found: %d", cmd.BrokerHeartbeat.BrokerID)
		}
		b.LastHeartbeat = time.Now().UTC()
		s.metadata.Brokers[b.ID] = b
	case CommandSetPartitionLeader:
		update := cmd.PartitionLeader
		topic, ok := s.metadata.Topics[update.Topic]
		if !ok {
			return ApplyResult{}, fmt.Errorf("topic not found: %s", update.Topic)
		}
		partition, ok := topic.Partitions[update.Partition]
		if !ok {
			return ApplyResult{}, fmt.Errorf("partition not found: topic=%s partition=%d", update.Topic, update.Partition)
		}
		partition.LeaderID = update.LeaderID
		if len(update.ISR) > 0 {
			partition.ISR = append([]int(nil), update.ISR...)
		}
		topic.Partitions[update.Partition] = partition
		s.metadata.Topics[update.Topic] = topic
	}

	s.index++
	return ApplyResult{Term: s.term, Index: s.index}, nil
}

func (s *RaftMetadataStore) Snapshot() MetadataSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metadata.clone()
}

func cloneTopic(topic TopicMetadata) TopicMetadata {
	partitions := make(map[int]PartitionMetadata, len(topic.Partitions))
	for id, p := range topic.Partitions {
		partitions[id] = PartitionMetadata{
			ID:       p.ID,
			LeaderID: p.LeaderID,
			Replicas: append([]int(nil), p.Replicas...),
			ISR:      append([]int(nil), p.ISR...),
		}
	}
	configs := make(map[string]string, len(topic.Configs))
	for k, v := range topic.Configs {
		configs[k] = v
	}
	return TopicMetadata{
		Name:       topic.Name,
		Partitions: partitions,
		Configs:    configs,
	}
}
