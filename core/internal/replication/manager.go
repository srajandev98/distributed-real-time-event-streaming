package replication

import (
	"fmt"
	"sync"
	"time"
)

// AlertHook allows callers to receive replication health transitions.
type AlertHook func(topic string, partition int, underReplicated bool, isrSize int)

// Config controls replication coordinator behavior.
type Config struct {
	ReplicationFactor int
	MinInSyncReplicas int
	MaxReplicaLag     int
	ReplicaLagTimeout time.Duration
	AckAllTimeout     time.Duration
}

// Status is a snapshot of one partition's replication state.
type Status struct {
	Topic             string
	Partition         int
	Role              string
	LeaderOffset      int
	HighWatermark     int
	InSyncReplicas    []int
	UnderReplicated   bool
	ReplicationFactor int
}

type replicaState struct {
	offset   int
	lastSeen time.Time
	inSync   bool
}

type partitionState struct {
	role          string
	leaderOffset  int
	highWatermark int
	followers     map[int]*replicaState
}

// Manager tracks ISR, lag and high watermark for partitions.
type Manager struct {
	cfg        Config
	mu         sync.Mutex
	partitions map[string]map[int]*partitionState
	alertHook  AlertHook
}

func NewManager(cfg Config, hook AlertHook) *Manager {
	if cfg.ReplicationFactor < 1 {
		cfg.ReplicationFactor = 1
	}
	if cfg.MinInSyncReplicas < 1 {
		cfg.MinInSyncReplicas = 1
	}
	if cfg.MinInSyncReplicas > cfg.ReplicationFactor {
		cfg.MinInSyncReplicas = cfg.ReplicationFactor
	}
	if cfg.MaxReplicaLag < 0 {
		cfg.MaxReplicaLag = 0
	}
	if cfg.ReplicaLagTimeout <= 0 {
		cfg.ReplicaLagTimeout = 10 * time.Second
	}
	if cfg.AckAllTimeout <= 0 {
		cfg.AckAllTimeout = 2 * time.Second
	}

	return &Manager{
		cfg:        cfg,
		partitions: make(map[string]map[int]*partitionState),
		alertHook:  hook,
	}
}

func (m *Manager) ensurePartition(topic string, partition int) *partitionState {
	if _, ok := m.partitions[topic]; !ok {
		m.partitions[topic] = make(map[int]*partitionState)
	}
	if _, ok := m.partitions[topic][partition]; !ok {
		followers := make(map[int]*replicaState)
		for id := 1; id < m.cfg.ReplicationFactor; id++ {
			followers[id] = &replicaState{offset: -1, inSync: false}
		}
		m.partitions[topic][partition] = &partitionState{
			role:          "leader",
			leaderOffset:  -1,
			highWatermark: -1,
			followers:     followers,
		}
	}
	return m.partitions[topic][partition]
}

// OnLeaderAppend records leader writes and refreshes ISR/HW state.
func (m *Manager) OnLeaderAppend(topic string, partition int, offset int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ps := m.ensurePartition(topic, partition)
	if ps.role != "leader" {
		return
	}
	if offset > ps.leaderOffset {
		ps.leaderOffset = offset
	}
	m.recomputeLocked(topic, partition, ps, time.Now())
}

// AckReplica records follower replication progress for a partition.
func (m *Manager) AckReplica(topic string, partition int, replicaID int, offset int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if replicaID <= 0 || replicaID >= m.cfg.ReplicationFactor {
		return fmt.Errorf("invalid replica id")
	}

	ps := m.ensurePartition(topic, partition)
	if ps.role != "leader" {
		return fmt.Errorf("partition is not leader")
	}
	r := ps.followers[replicaID]
	if r == nil {
		return fmt.Errorf("replica id not configured")
	}

	if offset > ps.leaderOffset {
		offset = ps.leaderOffset
	}
	if offset > r.offset {
		r.offset = offset
	}
	r.lastSeen = time.Now()
	m.recomputeLocked(topic, partition, ps, time.Now())
	return nil
}

// WaitForAckAll blocks until target offset is committed to ISR or timeout.
func (m *Manager) WaitForAckAll(topic string, partition int, targetOffset int) error {
	deadline := time.Now().Add(m.cfg.AckAllTimeout)
	for {
		m.mu.Lock()
		ps := m.ensurePartition(topic, partition)
		now := time.Now()
		m.recomputeLocked(topic, partition, ps, now)
		status := m.statusLocked(topic, partition, ps, now)
		m.mu.Unlock()

		if status.HighWatermark >= targetOffset && len(status.InSyncReplicas) >= m.cfg.MinInSyncReplicas {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acks=all timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Manager) HighWatermark(topic string, partition int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.ensurePartition(topic, partition)
	m.recomputeLocked(topic, partition, ps, time.Now())
	return ps.highWatermark
}

func (m *Manager) IsLeader(topic string, partition int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.ensurePartition(topic, partition)
	return ps.role == "leader"
}

func (m *Manager) SetRole(topic string, partition int, role string) error {
	if role != "leader" && role != "follower" {
		return fmt.Errorf("invalid role")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.ensurePartition(topic, partition)
	ps.role = role
	if role == "follower" {
		ps.highWatermark = -1
		for _, r := range ps.followers {
			r.inSync = false
		}
	}
	m.recomputeLocked(topic, partition, ps, time.Now())
	return nil
}

func (m *Manager) Status(topic string, partition int) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.ensurePartition(topic, partition)
	now := time.Now()
	m.recomputeLocked(topic, partition, ps, now)
	return m.statusLocked(topic, partition, ps, now)
}

func (m *Manager) statusLocked(topic string, partition int, ps *partitionState, now time.Time) Status {
	isr := []int{0}
	for id, r := range ps.followers {
		if r.inSync && now.Sub(r.lastSeen) <= m.cfg.ReplicaLagTimeout {
			isr = append(isr, id)
		}
	}
	return Status{
		Topic:             topic,
		Partition:         partition,
		Role:              ps.role,
		LeaderOffset:      ps.leaderOffset,
		HighWatermark:     ps.highWatermark,
		InSyncReplicas:    isr,
		UnderReplicated:   len(isr) < m.cfg.MinInSyncReplicas,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}
}

func (m *Manager) recomputeLocked(topic string, partition int, ps *partitionState, now time.Time) {
	isrOffsets := []int{ps.leaderOffset}
	isrSize := 1
	for _, r := range ps.followers {
		lag := ps.leaderOffset - r.offset
		fresh := !r.lastSeen.IsZero() && now.Sub(r.lastSeen) <= m.cfg.ReplicaLagTimeout
		r.inSync = fresh && lag <= m.cfg.MaxReplicaLag
		if r.inSync {
			isrSize++
			isrOffsets = append(isrOffsets, r.offset)
		}
	}

	hw := ps.leaderOffset
	for _, off := range isrOffsets {
		if off < hw {
			hw = off
		}
	}
	if hw < -1 {
		hw = -1
	}
	ps.highWatermark = hw

	if m.alertHook != nil {
		m.alertHook(topic, partition, isrSize < m.cfg.MinInSyncReplicas, isrSize)
	}
}
