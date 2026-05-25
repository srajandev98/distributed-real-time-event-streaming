package broker

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"flux/internal/config"
	"flux/internal/controlplane"
	"flux/internal/logging"
	"flux/internal/replication"
)

type peerInfo struct {
	ID   int
	Addr string
}

type replicaWorkerState struct {
	worker       *replication.FollowerWorker
	leaderID     int
	leaderAddr   string
	failureCount int
}

type clusterRuntime struct {
	broker                 *Broker
	peers                  map[int]peerInfo
	mu                     sync.Mutex
	workers                map[string]*replicaWorkerState
	done                   chan struct{}
	wg                     sync.WaitGroup
	reconcileEvery         time.Duration
	replicationPollEvery   time.Duration
	maxFailureBeforeFailover int
}

func newClusterRuntime(b *Broker, cfg *config.Config) *clusterRuntime {
	peers := parsePeers(cfg.ClusterPeers)
	peers[b.LocalBrokerID] = peerInfo{ID: b.LocalBrokerID, Addr: fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.AdvertisedPort)}
	return &clusterRuntime{
		broker:                   b,
		peers:                    peers,
		workers:                  make(map[string]*replicaWorkerState),
		done:                     make(chan struct{}),
		reconcileEvery:           1 * time.Second,
		replicationPollEvery:     100 * time.Millisecond,
		maxFailureBeforeFailover: 10,
	}
}

func parsePeers(raw []string) map[int]peerInfo {
	out := make(map[int]peerInfo)
	for _, entry := range raw {
		parts := strings.SplitN(strings.TrimSpace(entry), "@", 2)
		if len(parts) != 2 {
			continue
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil || id < 0 {
			continue
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			continue
		}
		out[id] = peerInfo{ID: id, Addr: addr}
	}
	return out
}

func workerKey(topic string, partition int) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func (r *clusterRuntime) start() {
	r.bootstrapPeers()
	r.wg.Add(1)
	go r.run()
}

func (r *clusterRuntime) stop() {
	close(r.done)
	r.wg.Wait()
}

func (r *clusterRuntime) run() {
	defer r.wg.Done()
	reconcileTicker := time.NewTicker(r.reconcileEvery)
	defer reconcileTicker.Stop()
	replicationTicker := time.NewTicker(r.replicationPollEvery)
	defer replicationTicker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-reconcileTicker.C:
			r.reconcileRoles()
		case <-replicationTicker.C:
			r.stepWorkers()
		}
	}
}

func (r *clusterRuntime) bootstrapPeers() {
	for id, peer := range r.peers {
		host, port, ok := splitHostPort(peer.Addr)
		if !ok {
			continue
		}
		_, _ = r.broker.Controller.RegisterBroker(id, host, port, time.Now().UnixNano())
	}
}

func splitHostPort(addr string) (string, int, bool) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return "", 0, false
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil || port <= 0 {
		return "", 0, false
	}
	host := strings.TrimSpace(parts[0])
	if host == "" {
		return "", 0, false
	}
	return host, port, true
}

func (r *clusterRuntime) reconcileRoles() {
	_ = r.broker.HeartbeatSelf()
	snapshot := r.broker.Controller.Snapshot()
	for topic, md := range snapshot.Topics {
		for partition, pm := range md.Partitions {
			lastOffset := r.broker.Storage.LastOffset(topic, partition)
			if pm.LeaderID == r.broker.LocalBrokerID {
				_ = r.broker.Replication.SetRole(topic, partition, "leader")
				r.broker.Replication.BootstrapOffset(topic, partition, lastOffset)
				r.stopWorker(topic, partition)
				continue
			}
			if !containsInt(pm.Replicas, r.broker.LocalBrokerID) || pm.LeaderID < 0 {
				r.stopWorker(topic, partition)
				continue
			}

			_ = r.broker.Replication.SetRole(topic, partition, "follower")
			r.broker.Replication.BootstrapOffset(topic, partition, lastOffset)
			leader, ok := r.peers[pm.LeaderID]
			if !ok {
				continue
			}
			r.ensureWorker(topic, partition, pm.LeaderID, leader.Addr, lastOffset+1)
		}
	}
}

func (r *clusterRuntime) ensureWorker(topic string, partition int, leaderID int, leaderAddr string, startOffset int) {
	key := workerKey(topic, partition)
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.workers[key]; ok {
		if existing.leaderID == leaderID && existing.leaderAddr == leaderAddr {
			return
		}
	}

	transport := replication.NewTCPTransport(leaderAddr, 2*time.Second)
	worker, err := replication.NewFollowerWorker(replication.WorkerConfig{
		FollowerID:  r.broker.LocalBrokerID,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
		MaxBatch:    256,
	}, transport, r.broker.Storage)
	if err != nil {
		logging.Warn("follower worker create failed", "topic", topic, "partition", partition, "error", err)
		return
	}
	r.workers[key] = &replicaWorkerState{
		worker:     worker,
		leaderID:   leaderID,
		leaderAddr: leaderAddr,
	}
}

func (r *clusterRuntime) stopWorker(topic string, partition int) {
	key := workerKey(topic, partition)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, key)
}

func (r *clusterRuntime) stepWorkers() {
	r.mu.Lock()
	keys := make([]string, 0, len(r.workers))
	for key := range r.workers {
		keys = append(keys, key)
	}
	workers := make(map[string]*replicaWorkerState, len(r.workers))
	for key, state := range r.workers {
		workers[key] = state
	}
	r.mu.Unlock()

	for _, key := range keys {
		state := workers[key]
		applied, err := state.worker.StepOnce()
		if err != nil {
			state.failureCount++
			logging.Warn("replication step failed", "worker", key, "leader_id", state.leaderID, "failure_count", state.failureCount, "error", err)
			if state.failureCount >= r.maxFailureBeforeFailover {
				r.tryFailoverForWorker(state.leaderID)
				state.failureCount = 0
			}
			continue
		}
		if applied > 0 {
			state.failureCount = 0
		}
	}
}

func (r *clusterRuntime) tryFailoverForWorker(leaderID int) {
	_, _ = r.broker.Controller.SetBrokerFence(leaderID, true)
	snapshot := r.broker.Controller.Snapshot()
	for topic, tm := range snapshot.Topics {
		changed, err := r.broker.Controller.ElectTopicLeaders(topic)
		if err != nil {
			continue
		}
		if changed > 0 {
			logging.Warn("leader election triggered after replication failures", "topic", topic, "changes", changed)
		}
		for partition, pm := range tm.Partitions {
			if pm.LeaderID == r.broker.LocalBrokerID {
				_ = r.broker.Replication.SetRole(topic, partition, "leader")
			}
		}
	}
}

func containsInt(items []int, target int) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func (b *Broker) HeartbeatSelf() error {
	_, err := b.Controller.HeartbeatBroker(b.LocalBrokerID)
	return err
}

func assignInitialLeaders(topic controlplane.TopicMetadata, brokers []int) map[int]int {
	out := make(map[int]int, len(topic.Partitions))
	if len(brokers) == 0 {
		return out
	}
	for partitionID := range topic.Partitions {
		out[partitionID] = brokers[partitionID%len(brokers)]
	}
	return out
}
