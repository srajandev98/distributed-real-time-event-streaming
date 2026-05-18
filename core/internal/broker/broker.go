package broker

import (
	"time"

	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/coordinator"
	"real-time-event-streaming/internal/logging"
	"real-time-event-streaming/internal/replication"
	"real-time-event-streaming/internal/storage"
)

// Broker is the composition root for runtime services used by request handlers.
type Broker struct {
	Storage     *storage.Storage
	Coordinator *coordinator.Coordinator
	Replication *replication.Manager
}

// NewBroker wires storage + coordinator using shared runtime config.
func NewBroker(cfg *config.Config) *Broker {
	replicationManager := replication.NewManager(replication.Config{
		ReplicationFactor: cfg.ReplicationFactor,
		MinInSyncReplicas: cfg.MinInSyncReplicas,
		MaxReplicaLag:     cfg.ReplicaMaxLag,
		ReplicaLagTimeout: time.Duration(cfg.ReplicaLagTimeoutMs) * time.Millisecond,
		AckAllTimeout:     time.Duration(cfg.AckAllTimeoutMs) * time.Millisecond,
	}, func(topic string, partition int, underReplicated bool, isrSize int) {
		if underReplicated {
			logging.Warn("under replicated partition detected", "topic", topic, "partition", partition, "isr_size", isrSize)
		}
	})

	return &Broker{
		Storage:     storage.NewStorage(cfg),
		Coordinator: coordinator.NewCoordinator(cfg),
		Replication: replicationManager,
	}
}
