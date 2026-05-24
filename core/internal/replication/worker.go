package replication

import "fmt"

// ReplicationRecord is one leader-fetched record to apply on follower storage.
type ReplicationRecord struct {
	Offset int
	Value  string
}

// FetchResult is one broker-fetch response from a leader.
type FetchResult struct {
	HighWatermark int
	Records       []ReplicationRecord
}

// Transport defines inter-broker replication RPCs used by follower workers.
type Transport interface {
	Fetch(topic string, partition int, followerID int, offset int, maxMessages int) (FetchResult, error)
	Ack(topic string, partition int, followerID int, ackedOffset int) error
}

// ReplicaStorage is the follower-local durable store apply surface.
type ReplicaStorage interface {
	AppendReplicated(topic string, partition int, offset int, value string) error
}

// WorkerConfig configures one follower replication loop for one partition.
type WorkerConfig struct {
	FollowerID  int
	Topic       string
	Partition   int
	StartOffset int
	MaxBatch    int
}

// FollowerWorker performs fetch -> apply -> ack for one follower partition replica.
type FollowerWorker struct {
	transport  Transport
	storage    ReplicaStorage
	followerID int
	topic      string
	partition  int
	nextOffset int
	maxBatch   int
}

func NewFollowerWorker(cfg WorkerConfig, transport Transport, storage ReplicaStorage) (*FollowerWorker, error) {
	if transport == nil {
		return nil, fmt.Errorf("transport is required")
	}
	if storage == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if cfg.FollowerID <= 0 {
		return nil, fmt.Errorf("follower id must be > 0")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if cfg.Partition < 0 {
		return nil, fmt.Errorf("partition must be >= 0")
	}
	if cfg.StartOffset < 0 {
		return nil, fmt.Errorf("start offset must be >= 0")
	}
	maxBatch := cfg.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 100
	}

	return &FollowerWorker{
		transport:  transport,
		storage:    storage,
		followerID: cfg.FollowerID,
		topic:      cfg.Topic,
		partition:  cfg.Partition,
		nextOffset: cfg.StartOffset,
		maxBatch:   maxBatch,
	}, nil
}

// StepOnce runs one full replication cycle:
// 1) fetch from leader at nextOffset
// 2) apply fetched records to local follower storage
// 3) ack latest applied offset back to leader
func (w *FollowerWorker) StepOnce() (int, error) {
	result, err := w.transport.Fetch(w.topic, w.partition, w.followerID, w.nextOffset, w.maxBatch)
	if err != nil {
		return 0, err
	}
	if len(result.Records) == 0 {
		return 0, nil
	}

	applied := 0
	lastApplied := -1
	for _, record := range result.Records {
		if record.Offset != w.nextOffset {
			return applied, fmt.Errorf("replication gap: expected offset=%d got=%d", w.nextOffset, record.Offset)
		}
		if err := w.storage.AppendReplicated(w.topic, w.partition, record.Offset, record.Value); err != nil {
			return applied, err
		}
		w.nextOffset++
		lastApplied = record.Offset
		applied++
	}

	if lastApplied >= 0 {
		if err := w.transport.Ack(w.topic, w.partition, w.followerID, lastApplied); err != nil {
			return applied, err
		}
	}
	return applied, nil
}

func (w *FollowerWorker) NextOffset() int {
	return w.nextOffset
}
