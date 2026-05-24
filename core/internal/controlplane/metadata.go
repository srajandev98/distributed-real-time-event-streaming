package controlplane

import "time"

// MetadataSnapshot is a read-only view of cluster metadata.
type MetadataSnapshot struct {
	Topics  map[string]TopicMetadata
	Brokers map[int]BrokerMetadata
}

// TopicMetadata stores partition layout + topic-level configuration.
type TopicMetadata struct {
	Name       string
	Partitions map[int]PartitionMetadata
	Configs    map[string]string
}

// PartitionMetadata stores replica placement and leadership.
type PartitionMetadata struct {
	ID       int
	LeaderID int
	Replicas []int
	ISR      []int
}

// BrokerMetadata tracks broker registration and liveness state.
type BrokerMetadata struct {
	ID            int
	Host          string
	Port          int
	Epoch         int64
	LastHeartbeat time.Time
	Fenced        bool
}

func (s MetadataSnapshot) clone() MetadataSnapshot {
	out := MetadataSnapshot{
		Topics:  make(map[string]TopicMetadata, len(s.Topics)),
		Brokers: make(map[int]BrokerMetadata, len(s.Brokers)),
	}
	for name, topic := range s.Topics {
		partitions := make(map[int]PartitionMetadata, len(topic.Partitions))
		for pid, p := range topic.Partitions {
			partitions[pid] = PartitionMetadata{
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
		out.Topics[name] = TopicMetadata{
			Name:       topic.Name,
			Partitions: partitions,
			Configs:    configs,
		}
	}
	for id, broker := range s.Brokers {
		out.Brokers[id] = broker
	}
	return out
}
