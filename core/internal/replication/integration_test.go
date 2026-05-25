package replication_test

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"flux/internal/broker"
	"flux/internal/config"
	"flux/internal/network"
	"flux/internal/replication"
	"flux/internal/storage"
)

func TestFollowerWorkerWithTCPTransport(t *testing.T) {
	leaderCfg := &config.Config{
		ListenAddr:             ":0",
		DataDir:                t.TempDir(),
		NumPartitions:          3,
		ReplicationFactor:      3,
		MinInSyncReplicas:      2,
		ReplicaMaxLag:          0,
		ReplicaLagTimeoutMs:    10_000,
		AckAllTimeoutMs:        500,
		SegmentMaxBytes:        1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}
	leaderBroker := broker.NewBroker(leaderCfg)
	leaderAddr, shutdown := startTestBrokerServer(t, leaderBroker)
	defer shutdown()

	// Produce one record to leader.
	produceResp, err := sendLineCommand(leaderAddr, "PRODUCE", "orders user-x:created acks=1")
	if err != nil {
		t.Fatalf("produce command failed: %v", err)
	}
	partition, err := parsePartitionField(produceResp)
	if err != nil {
		t.Fatalf("parse partition failed: %v, response=%s", err, produceResp)
	}

	// Follower-side storage + replication worker.
	followerCfg := &config.Config{
		ListenAddr:             ":0",
		DataDir:                t.TempDir(),
		NumPartitions:          3,
		SegmentMaxBytes:        1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}
	followerStorage := storage.NewStorage(followerCfg)
	transport := replication.NewTCPTransport(leaderAddr, 2*time.Second)
	worker, err := replication.NewFollowerWorker(replication.WorkerConfig{
		FollowerID:  1,
		Topic:       "orders",
		Partition:   partition,
		StartOffset: 0,
		MaxBatch:    10,
	}, transport, followerStorage)
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}

	applied, err := worker.StepOnce()
	if err != nil {
		t.Fatalf("worker step failed: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected 1 replicated record applied, got %d", applied)
	}

	// Verify follower local apply.
	messages := followerStorage.Consume("orders", partition, 0)
	if len(messages) != 1 || messages[0].Value != "created" {
		t.Fatalf("unexpected follower messages: %#v", messages)
	}

	// Verify leader received follower ack and reflected ISR/HW state.
	status := leaderBroker.Replication.Status("orders", partition)
	foundReplica1 := false
	for _, replicaID := range status.InSyncReplicas {
		if replicaID == 1 {
			foundReplica1 = true
			break
		}
	}
	if !foundReplica1 {
		t.Fatalf("expected replica 1 in ISR after ack, status=%+v", status)
	}
}

func TestClusterRuntimeReplicatesAcrossBrokersAndFailsOver(t *testing.T) {
	leaderCfg := &config.Config{
		ListenAddr:             ":0",
		AdvertisedHost:         "127.0.0.1",
		AdvertisedPort:         19092,
		BrokerID:               0,
		ClusterPeers:           []string{"0@127.0.0.1:19092", "1@127.0.0.1:19093"},
		DataDir:                t.TempDir(),
		NumPartitions:          1,
		ReplicationFactor:      2,
		MinInSyncReplicas:      2,
		ReplicaMaxLag:          0,
		ReplicaLagTimeoutMs:    10_000,
		AckAllTimeoutMs:        1000,
		SegmentMaxBytes:        1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}
	followerCfg := &config.Config{
		ListenAddr:             ":0",
		AdvertisedHost:         "127.0.0.1",
		AdvertisedPort:         19093,
		BrokerID:               1,
		ClusterPeers:           []string{"0@127.0.0.1:19092", "1@127.0.0.1:19093"},
		DataDir:                t.TempDir(),
		NumPartitions:          1,
		ReplicationFactor:      2,
		MinInSyncReplicas:      2,
		ReplicaMaxLag:          0,
		ReplicaLagTimeoutMs:    10_000,
		AckAllTimeoutMs:        1000,
		SegmentMaxBytes:        1024 * 1024,
		RetentionMaxBytes:      50 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        1000,
		FlushBytes:             64 * 1024,
		FsyncMode:              "always",
	}

	leaderBroker := broker.NewBroker(leaderCfg)
	followerBroker := broker.NewBroker(followerCfg)

	_, _ = leaderBroker.Controller.CreateTopic("orders", 1, 2)
	_, _ = followerBroker.Controller.CreateTopic("orders", 1, 2)
	_, _ = leaderBroker.Controller.SetPartitionLeader("orders", 0, 0, []int{0, 1})
	_, _ = followerBroker.Controller.SetPartitionLeader("orders", 0, 0, []int{0, 1})

	leaderBroker.StartBackgroundRuntimes()
	followerBroker.StartBackgroundRuntimes()
	defer leaderBroker.StopBackgroundRuntimes()
	defer followerBroker.StopBackgroundRuntimes()

	leaderAddr, shutdownLeader := startTestBrokerServer(t, leaderBroker)
	defer shutdownLeader()
	followerAddr, shutdownFollower := startTestBrokerServer(t, followerBroker)
	defer shutdownFollower()

	_, _ = followerAddr, leaderAddr

	resp, err := sendLineCommand(leaderAddr, "PRODUCE", "orders user-x:created acks=1")
	if err != nil {
		t.Fatalf("produce on leader failed: %v", err)
	}
	if !strings.Contains(resp, "|OK|partition=") {
		t.Fatalf("unexpected produce response: %s", resp)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		msgs := followerBroker.Storage.Consume("orders", 0, 0)
		if len(msgs) > 0 && msgs[0].Value == "created" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replication did not apply to follower before deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Simulate leader failure and verify follower promotion.
	shutdownLeader()

	deadline = time.Now().Add(5 * time.Second)
	for {
		s := followerBroker.Controller.Snapshot()
		if topic, ok := s.Topics["orders"]; ok {
			if topic.Partitions[0].LeaderID == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower was not promoted to leader after leader shutdown")
		}
		time.Sleep(100 * time.Millisecond)
	}

	resp, err = sendLineCommand(followerAddr, "PRODUCE", "orders user-y:paid acks=1")
	if err != nil {
		t.Fatalf("produce on promoted follower failed: %v", err)
	}
	if !strings.Contains(resp, "|OK|partition=0") {
		t.Fatalf("expected produce success on promoted leader, got: %s", resp)
	}
}

func startTestBrokerServer(t *testing.T, b *broker.Broker) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping integration test in restricted sandbox: %v", err)
		}
		t.Fatalf("listen failed: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go network.HandleConnection(conn, b)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func sendLineCommand(addr string, command string, args string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	line := fmt.Sprintf("V1|1|%s|%s\n", command, args)
	if _, err := conn.Write([]byte(line)); err != nil {
		return "", err
	}
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

func parsePartitionField(resp string) (int, error) {
	idx := strings.Index(resp, "partition=")
	if idx == -1 {
		return 0, fmt.Errorf("partition field missing")
	}
	rest := resp[idx+len("partition="):]
	end := strings.Index(rest, " ")
	if end == -1 {
		end = len(rest)
	}
	return strconv.Atoi(rest[:end])
}
