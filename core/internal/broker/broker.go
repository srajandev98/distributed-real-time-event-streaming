package broker

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"flux/internal/config"
	"flux/internal/controlplane"
	"flux/internal/coordinator"
	"flux/internal/logging"
	"flux/internal/replication"
	"flux/internal/storage"
)

// Broker is the composition root for runtime services used by request handlers.
type Broker struct {
	Storage                  *storage.Storage
	Coordinator              *coordinator.Coordinator
	Replication              *replication.Manager
	Controller               *controlplane.Controller
	LocalBrokerID            int
	DefaultPartitions        int
	DefaultReplicationFactor int
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

	b := &Broker{
		Storage:                  storage.NewStorage(cfg),
		Coordinator:              coordinator.NewCoordinator(cfg),
		Replication:              replicationManager,
		Controller:               controlplane.NewController(controlplane.NewRaftMetadataStore()),
		LocalBrokerID:            cfg.BrokerID,
		DefaultPartitions:        cfg.NumPartitions,
		DefaultReplicationFactor: cfg.ReplicationFactor,
	}
	b.bootstrapControllerMetadata(cfg)
	return b
}

func (b *Broker) bootstrapControllerMetadata(cfg *config.Config) {
	host := strings.TrimSpace(cfg.AdvertisedHost)
	port := cfg.AdvertisedPort
	if host == "" || port <= 0 {
		fallbackHost, fallbackPort := parseListenAddr(cfg.ListenAddr)
		if host == "" {
			host = fallbackHost
		}
		if port <= 0 {
			port = fallbackPort
		}
	}
	_, _ = b.Controller.RegisterBroker(b.LocalBrokerID, host, port, time.Now().UnixNano())
}

func parseListenAddr(listen string) (string, int) {
	host := "127.0.0.1"
	port := 9092
	addr := strings.TrimSpace(listen)
	if strings.HasPrefix(addr, ":") {
		if parsed, err := strconv.Atoi(strings.TrimPrefix(addr, ":")); err == nil && parsed > 0 {
			port = parsed
		}
		return host, port
	}
	if h, p, err := net.SplitHostPort(addr); err == nil {
		if h != "" {
			host = h
		}
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			port = parsed
		}
	}
	return host, port
}

// EnsureTopicMetadata migrates implicit local topic usage into controller metadata.
func (b *Broker) EnsureTopicMetadata(topic string) (controlplane.TopicMetadata, error) {
	if topic == "" {
		return controlplane.TopicMetadata{}, fmt.Errorf("topic is required")
	}
	snapshot := b.Controller.Snapshot()
	if md, ok := snapshot.Topics[topic]; ok {
		return md, nil
	}
	if _, err := b.Controller.CreateTopic(topic, b.DefaultPartitions, b.DefaultReplicationFactor); err != nil {
		return controlplane.TopicMetadata{}, err
	}
	for partition := 0; partition < b.DefaultPartitions; partition++ {
		if _, err := b.Controller.SetPartitionLeader(topic, partition, b.LocalBrokerID, []int{b.LocalBrokerID}); err != nil {
			return controlplane.TopicMetadata{}, err
		}
	}
	updated := b.Controller.Snapshot()
	return updated.Topics[topic], nil
}
