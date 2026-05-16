package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"real-time-event-streaming/internal/config"
)

// newTestStorage creates a small-config storage for fast unit tests.
func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	cfg := &config.Config{
		ListenAddr:             ":0",
		DataDir:                t.TempDir(),
		NumPartitions:          3,
		SegmentMaxBytes:        128,
		RetentionMaxBytes:      8 * 1024 * 1024,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        10,
		FlushBytes:             128,
		FsyncMode:              "always",
	}
	return NewStorage(cfg)
}

// TestProduceConsume verifies append + read from offset 0.
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

// TestConsumeBounds validates empty responses for missing/out-of-range reads.
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

// TestLoadDataRecovery verifies startup replay from segment files.
func TestLoadDataRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ListenAddr: ":0", DataDir: dir, NumPartitions: 3, SegmentMaxBytes: 1024, RetentionMaxBytes: 1 << 20, RetentionMaxAgeSeconds: 86400, FlushIntervalMs: 10, FlushBytes: 64, FsyncMode: "always"}

	line1 := fmt.Sprintf("0|%d|%08x|created\n", time.Now().UnixMilli(), crc32Of("created"))
	line2 := fmt.Sprintf("1|%d|%08x|paid\n", time.Now().UnixMilli(), crc32Of("paid"))
	path := filepath.Join(dir, "orders-1-segment-000000.log")
	if err := os.WriteFile(path, []byte(line1+line2), 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	s := NewStorage(cfg)
	messages := s.Consume("orders", 1, 0)
	if len(messages) != 2 {
		t.Fatalf("expected 2 recovered messages, got %d", len(messages))
	}
	if messages[1].Offset != 1 || messages[1].Value != "paid" {
		t.Fatalf("unexpected recovered message: %+v", messages[1])
	}
}

// TestSegmentRotationAndIndexes checks segment rollover and index creation.
func TestSegmentRotationAndIndexes(t *testing.T) {
	s := newTestStorage(t)
	for i := 0; i < 10; i++ {
		_, _ = s.Produce("orders", fmt.Sprintf("k-%d", i), strings.Repeat("x", 24))
	}

	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	segmentCount := 0
	hasOffsetIdx := false
	hasTimeIdx := false
	for _, f := range files {
		name := f.Name()
		if strings.Contains(name, "segment") && strings.HasSuffix(name, ".log") {
			segmentCount++
		}
		if strings.HasSuffix(name, "-offset.idx") {
			hasOffsetIdx = true
		}
		if strings.HasSuffix(name, "-time.idx") {
			hasTimeIdx = true
		}
	}

	if segmentCount < 2 {
		t.Fatalf("expected rotated segments, got %d", segmentCount)
	}
	if !hasOffsetIdx || !hasTimeIdx {
		t.Fatalf("expected both index files; offset=%v time=%v", hasOffsetIdx, hasTimeIdx)
	}
}

// TestLocalReplicaFilesCreated verifies simplified same-node replica file writes.
func TestLocalReplicaFilesCreated(t *testing.T) {
	s := newTestStorage(t)
	_, _ = s.Produce("orders", "same-key", "created")

	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	replica1 := false
	replica2 := false
	for _, f := range files {
		name := f.Name()
		if strings.Contains(name, "-replica-1-segment-") {
			replica1 = true
		}
		if strings.Contains(name, "-replica-2-segment-") {
			replica2 = true
		}
	}

	if !replica1 || !replica2 {
		t.Fatalf("expected replica files to be created; replica1=%v replica2=%v", replica1, replica2)
	}
}

// TestChecksumRecoverySkipsCorruptRecords ensures bad records are skipped.
func TestChecksumRecoverySkipsCorruptRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ListenAddr: ":0", DataDir: dir, NumPartitions: 3, SegmentMaxBytes: 1024, RetentionMaxBytes: 1 << 20, RetentionMaxAgeSeconds: 86400, FlushIntervalMs: 10, FlushBytes: 64, FsyncMode: "always"}

	good := fmt.Sprintf("0|%d|%08x|ok\n", time.Now().UnixMilli(), crc32Of("ok"))
	bad := fmt.Sprintf("1|%d|deadbeef|corrupt\n", time.Now().UnixMilli())
	path := filepath.Join(dir, "orders-0-segment-000000.log")
	if err := os.WriteFile(path, []byte(good+bad), 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	s := NewStorage(cfg)
	messages := s.Consume("orders", 0, 0)
	if len(messages) != 1 {
		t.Fatalf("expected only good record after checksum validation, got %d", len(messages))
	}
	if messages[0].Value != "ok" {
		t.Fatalf("expected recovered value ok, got %s", messages[0].Value)
	}
}

// TestRetentionBySize validates segment cleanup by max-size policy.
func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		ListenAddr:             ":0",
		DataDir:                dir,
		NumPartitions:          3,
		SegmentMaxBytes:        120,
		RetentionMaxBytes:      200,
		RetentionMaxAgeSeconds: 86400,
		FlushIntervalMs:        10,
		FlushBytes:             64,
		FsyncMode:              "always",
	}
	s := NewStorage(cfg)

	for i := 0; i < 15; i++ {
		_, _ = s.Produce("orders", "same-key", strings.Repeat("payload", 8))
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var total int64
	for _, f := range files {
		if strings.Contains(f.Name(), "segment") {
			info, _ := f.Info()
			total += info.Size()
		}
	}

	if total > cfg.RetentionMaxBytes+cfg.SegmentMaxBytes {
		t.Fatalf("expected retention limit enforcement, total=%d limit=%d (+active segment slack)", total, cfg.RetentionMaxBytes)
	}
}

// TestGetPartitionDeterministic ensures key hashing is stable.
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

// crc32Of is a local helper used to generate expected record checksums.
func crc32Of(value string) uint32 {
	var table = [256]uint32{}
	for i := 0; i < 256; i++ {
		crc := uint32(i)
		for j := 0; j < 8; j++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	crc := uint32(0xFFFFFFFF)
	for i := 0; i < len(value); i++ {
		crc = (crc >> 8) ^ table[(crc^uint32(value[i]))&0xFF]
	}
	return crc ^ 0xFFFFFFFF
}
