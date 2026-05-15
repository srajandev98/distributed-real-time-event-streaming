package storage

import (
	"os"
	"path/filepath"
	"testing"

	"real-time-event-streaming/internal/config"
)

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	cfg := &config.Config{
		ListenAddr:    ":0",
		DataDir:       t.TempDir(),
		NumPartitions: 3,
	}
	return NewStorage(cfg)
}

func TestProduceConsume(t *testing.T) {
	s := newTestStorage(t)

	partition, offset := s.Produce("orders", "user1", "created")
	if partition < 0 || partition >= 3 {
		t.Fatalf("invalid partition: %d", partition)
	}
	if offset != 0 {
		t.Fatalf("expected first offset 0, got %d", offset)
	}

	messages := s.Consume("orders", partition, 0)
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].Value != "created" || messages[0].Offset != 0 {
		t.Fatalf("unexpected message: %+v", messages[0])
	}
}

func TestConsumeBounds(t *testing.T) {
	s := newTestStorage(t)
	partition, _ := s.Produce("orders", "user2", "paid")

	messages := s.Consume("orders", partition, 1)
	if len(messages) != 0 {
		t.Fatalf("expected empty result for out-of-range offset, got %d", len(messages))
	}

	missingTopic := s.Consume("unknown", 0, 0)
	if len(missingTopic) != 0 {
		t.Fatalf("expected empty result for missing topic, got %d", len(missingTopic))
	}
}

func TestLoadDataRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders-1.log")
	content := "0:created\n1:paid\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	cfg := &config.Config{ListenAddr: ":0", DataDir: dir, NumPartitions: 3}
	s := NewStorage(cfg)

	messages := s.Consume("orders", 1, 0)
	if len(messages) != 2 {
		t.Fatalf("expected 2 recovered messages, got %d", len(messages))
	}
	if messages[1].Offset != 1 || messages[1].Value != "paid" {
		t.Fatalf("unexpected recovered message: %+v", messages[1])
	}
}

func TestGetPartitionDeterministic(t *testing.T) {
	p1 := getPartition("user-42", 3)
	p2 := getPartition("user-42", 3)

	if p1 != p2 {
		t.Fatalf("expected deterministic partition, got %d and %d", p1, p2)
	}
	if p1 < 0 || p1 >= 3 {
		t.Fatalf("partition out of range: %d", p1)
	}
}
