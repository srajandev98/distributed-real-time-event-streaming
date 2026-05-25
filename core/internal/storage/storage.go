package storage

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"flux/internal/config"
	"flux/internal/logging"
	"flux/internal/types"
)

const (
	// Reasonable defaults used when config values are not provided.
	defaultSegmentMaxBytes   int64         = 1 * 1024 * 1024
	defaultRetentionMaxAge   time.Duration = 24 * time.Hour
	defaultFlushIntervalMs   int           = 1000
	defaultFlushBytes        int64         = 64 * 1024
	defaultFsyncMode         string        = "always"
	fsyncModeAlways          string        = "always"
	fsyncModeNever           string        = "never"
	fsyncModeInterval        string        = "interval"
	defaultRetentionMaxByte  int64         = 50 * 1024 * 1024
	defaultLocalReplicaCount int           = 2
)

// partitionState tracks mutable write/retention state for one partition.
type partitionState struct {
	nextOffset      int
	activeSegmentID int
	segments        []int
	lastFsyncAt     time.Time
	pendingBytes    int64
}

// Storage owns durable logs, indexes, retention, and in-memory read cache.
type Storage struct {
	data              map[string]map[int][]types.Message
	dataDir           string
	numPartitions     int
	segmentMaxBytes   int64
	retentionMaxBytes int64
	retentionMaxAge   time.Duration
	flushInterval     time.Duration
	flushBytes        int64
	fsyncMode         string
	partitionState    map[string]map[int]*partitionState
	localReplicaCount int
	mutex             sync.Mutex
}

// NewStorage initializes storage settings and recovers existing data from disk.
func NewStorage(cfg *config.Config) *Storage {
	s := &Storage{
		data:              make(map[string]map[int][]types.Message),
		dataDir:           cfg.DataDir,
		numPartitions:     cfg.NumPartitions,
		segmentMaxBytes:   valueOrDefaultInt64(cfg.SegmentMaxBytes, defaultSegmentMaxBytes),
		retentionMaxBytes: valueOrDefaultInt64(cfg.RetentionMaxBytes, defaultRetentionMaxByte),
		retentionMaxAge:   valueOrDefaultDuration(time.Duration(cfg.RetentionMaxAgeSeconds)*time.Second, defaultRetentionMaxAge),
		flushInterval:     valueOrDefaultDuration(time.Duration(cfg.FlushIntervalMs)*time.Millisecond, time.Duration(defaultFlushIntervalMs)*time.Millisecond),
		flushBytes:        valueOrDefaultInt64(cfg.FlushBytes, defaultFlushBytes),
		fsyncMode:         valueOrDefaultString(cfg.FsyncMode, defaultFsyncMode),
		partitionState:    make(map[string]map[int]*partitionState),
		localReplicaCount: defaultLocalReplicaCount,
	}

	s.loadData()
	s.enforceRetentionLocked()
	return s
}

// Produce appends a message to the chosen partition and returns partition+offset.
func (s *Storage) Produce(topic string, key string, value string) (int, int) {
	partition := getPartition(key, s.numPartitions)
	return s.ProduceToPartition(topic, partition, value)
}

// ProduceToPartition appends a message to an explicit partition.
func (s *Storage) ProduceToPartition(topic string, partition int, value string) (int, int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	st := s.ensurePartitionState(topic, partition)
	offset := st.nextOffset
	timestamp := time.Now().UnixMilli()
	checksum := crc32.ChecksumIEEE([]byte(value))
	line := fmt.Sprintf("%d|%d|%08x|%s\n", offset, timestamp, checksum, value)

	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		logging.Error("storage mkdir failed", "data_dir", s.dataDir, "error", err)
		return partition, offset
	}

	// Rotate to a new segment when current segment crosses size threshold.
	if s.shouldRotate(topic, partition, st.activeSegmentID, int64(len(line))) {
		st.activeSegmentID++
		st.segments = append(st.segments, st.activeSegmentID)
	}

	segmentPath := s.segmentPath(topic, partition, st.activeSegmentID)
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		logging.Error("open segment failed", "path", segmentPath, "error", err)
		return partition, offset
	}

	if _, err := f.WriteString(line); err != nil {
		_ = f.Close()
		logging.Error("write segment failed", "path", segmentPath, "error", err)
		return partition, offset
	}

	if err := s.applyFsyncPolicy(f, st, int64(len(line))); err != nil {
		logging.Error("fsync failed", "path", segmentPath, "error", err)
	}
	_ = f.Close()

	// Simplified local replication: mirror records into replica files on same node.
	s.writeLocalReplicas(topic, partition, st.activeSegmentID, line)

	// Keep simple side indexes to make future seek/read optimizations possible.
	s.appendOffsetIndex(topic, partition, offset, st.activeSegmentID)
	s.appendTimestampIndex(topic, partition, timestamp, st.activeSegmentID, offset)

	if _, exists := s.data[topic]; !exists {
		s.data[topic] = make(map[int][]types.Message)
	}
	s.data[topic][partition] = append(s.data[topic][partition], types.Message{Offset: offset, Value: value})
	st.nextOffset++

	// Enforce cleanup policy after appends so data size/age stays bounded.
	s.enforceRetentionLocked()
	return partition, offset
}

// AppendReplicated appends a follower-replicated record at an explicit offset.
// This is used by follower replication loops applying leader-fetched records.
func (s *Storage) AppendReplicated(topic string, partition int, offset int, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	st := s.ensurePartitionState(topic, partition)
	if offset < st.nextOffset {
		// Idempotent replay: already applied.
		return nil
	}
	if offset > st.nextOffset {
		return fmt.Errorf("non-contiguous replicated offset: expected=%d got=%d", st.nextOffset, offset)
	}

	timestamp := time.Now().UnixMilli()
	checksum := crc32.ChecksumIEEE([]byte(value))
	line := fmt.Sprintf("%d|%d|%08x|%s\n", offset, timestamp, checksum, value)

	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return err
	}
	if s.shouldRotate(topic, partition, st.activeSegmentID, int64(len(line))) {
		st.activeSegmentID++
		st.segments = append(st.segments, st.activeSegmentID)
	}

	segmentPath := s.segmentPath(topic, partition, st.activeSegmentID)
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(line); err != nil {
		_ = f.Close()
		return err
	}
	if err := s.applyFsyncPolicy(f, st, int64(len(line))); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()

	s.appendOffsetIndex(topic, partition, offset, st.activeSegmentID)
	s.appendTimestampIndex(topic, partition, timestamp, st.activeSegmentID, offset)

	if _, exists := s.data[topic]; !exists {
		s.data[topic] = make(map[int][]types.Message)
	}
	s.data[topic][partition] = append(s.data[topic][partition], types.Message{Offset: offset, Value: value})
	st.nextOffset = offset + 1
	s.enforceRetentionLocked()
	return nil
}

// PartitionForKeyWithCount hashes key to a deterministic partition count.
func (s *Storage) PartitionForKeyWithCount(key string, partitionCount int) int {
	return getPartition(key, partitionCount)
}

// PartitionForKey returns the deterministic partition for a producer key.
func (s *Storage) PartitionForKey(key string) int {
	return getPartition(key, s.numPartitions)
}

// LastOffset returns the last known local offset for topic/partition, or -1.
func (s *Storage) LastOffset(topic string, partition int) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	partitions, ok := s.data[topic]
	if !ok {
		return -1
	}
	messages, ok := partitions[partition]
	if !ok || len(messages) == 0 {
		return -1
	}
	return messages[len(messages)-1].Offset
}

func (s *Storage) writeLocalReplicas(topic string, partition int, segmentID int, line string) {
	for replicaID := 1; replicaID <= s.localReplicaCount; replicaID++ {
		path := s.replicaSegmentPath(topic, partition, replicaID, segmentID)
		if err := appendLine(path, line); err != nil {
			logging.Error("write replica segment failed", "path", path, "error", err)
		}
	}
}

// Consume returns messages from a partition starting at provided offset.
func (s *Storage) Consume(topic string, partition int, offset int) []types.Message {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	partitions, exists := s.data[topic]
	if !exists {
		return []types.Message{}
	}

	messages, exists := partitions[partition]
	if !exists {
		return []types.Message{}
	}

	if offset >= len(messages) {
		return []types.Message{}
	}

	return messages[offset:]
}

// getPartition hashes key to a deterministic partition.
func getPartition(key string, numPartitions int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32()) % numPartitions
}

// loadData recovers all segment files on startup in deterministic order.
func (s *Storage) loadData() {
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return
	}

	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		return
	}

	entries := make([]segmentEntry, 0)
	for _, file := range files {
		topic, partition, segID, ok := parseSegmentFileName(file.Name())
		if !ok {
			continue
		}
		entries = append(entries, segmentEntry{topic: topic, partition: partition, segmentID: segID, path: filepath.Join(s.dataDir, file.Name())})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].topic != entries[j].topic {
			return entries[i].topic < entries[j].topic
		}
		if entries[i].partition != entries[j].partition {
			return entries[i].partition < entries[j].partition
		}
		return entries[i].segmentID < entries[j].segmentID
	})

	for _, e := range entries {
		st := s.ensurePartitionState(e.topic, e.partition)
		if len(st.segments) == 0 || st.segments[len(st.segments)-1] != e.segmentID {
			st.segments = append(st.segments, e.segmentID)
		}
		if e.segmentID > st.activeSegmentID {
			st.activeSegmentID = e.segmentID
		}
		s.loadSegment(e.topic, e.partition, e.segmentID, e.path)
	}
}

type segmentEntry struct {
	topic     string
	partition int
	segmentID int
	path      string
}

// loadSegment replays one segment file into in-memory cache and indexes.
func (s *Storage) loadSegment(topic string, partition int, segmentID int, path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		offset, ts, val, ok := parseLogLine(line)
		if !ok {
			continue
		}

		if _, exists := s.data[topic]; !exists {
			s.data[topic] = make(map[int][]types.Message)
		}
		s.data[topic][partition] = append(s.data[topic][partition], types.Message{Offset: offset, Value: val})

		st := s.ensurePartitionState(topic, partition)
		if st.nextOffset <= offset {
			st.nextOffset = offset + 1
		}

		s.appendOffsetIndex(topic, partition, offset, segmentID)
		s.appendTimestampIndex(topic, partition, ts, segmentID, offset)
	}
}

// parseLogLine validates one persisted line and verifies checksum integrity.
func parseLogLine(line string) (int, int64, string, bool) {
	parts := strings.SplitN(line, "|", 4)
	if len(parts) != 4 {
		return 0, 0, "", false
	}
	offset, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, "", false
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	providedChecksum := parts[2]
	value := parts[3]
	computed := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(value)))
	if !strings.EqualFold(computed, providedChecksum) {
		logging.Warn("checksum mismatch; skipping record", "offset", offset)
		return 0, 0, "", false
	}
	return offset, ts, value, true
}

// ensurePartitionState lazily creates partition runtime metadata.
func (s *Storage) ensurePartitionState(topic string, partition int) *partitionState {
	if _, ok := s.partitionState[topic]; !ok {
		s.partitionState[topic] = make(map[int]*partitionState)
	}
	if _, ok := s.partitionState[topic][partition]; !ok {
		s.partitionState[topic][partition] = &partitionState{
			nextOffset:      0,
			activeSegmentID: 0,
			segments:        []int{0},
			lastFsyncAt:     time.Now(),
		}
	}
	return s.partitionState[topic][partition]
}

// shouldRotate checks if appending would exceed configured segment size.
func (s *Storage) shouldRotate(topic string, partition int, segmentID int, appendBytes int64) bool {
	path := s.segmentPath(topic, partition, segmentID)
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Size()+appendBytes > s.segmentMaxBytes
}

func (s *Storage) segmentPath(topic string, partition int, segmentID int) string {
	return filepath.Join(s.dataDir, fmt.Sprintf("%s-%d-segment-%06d.log", topic, partition, segmentID))
}

func (s *Storage) offsetIndexPath(topic string, partition int) string {
	return filepath.Join(s.dataDir, fmt.Sprintf("%s-%d-offset.idx", topic, partition))
}

func (s *Storage) timestampIndexPath(topic string, partition int) string {
	return filepath.Join(s.dataDir, fmt.Sprintf("%s-%d-time.idx", topic, partition))
}

func (s *Storage) replicaSegmentPath(topic string, partition int, replicaID int, segmentID int) string {
	return filepath.Join(s.dataDir, fmt.Sprintf("%s-%d-replica-%d-segment-%06d.log", topic, partition, replicaID, segmentID))
}

func (s *Storage) appendOffsetIndex(topic string, partition int, offset int, segmentID int) {
	path := s.offsetIndexPath(topic, partition)
	line := fmt.Sprintf("%d:%06d\n", offset, segmentID)
	_ = appendLine(path, line)
}

func (s *Storage) appendTimestampIndex(topic string, partition int, ts int64, segmentID int, offset int) {
	path := s.timestampIndexPath(topic, partition)
	line := fmt.Sprintf("%d:%06d:%d\n", ts, segmentID, offset)
	_ = appendLine(path, line)
}

func appendLine(path string, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(line)
	return err
}

// applyFsyncPolicy controls durability behavior based on configured mode.
func (s *Storage) applyFsyncPolicy(f *os.File, st *partitionState, written int64) error {
	switch s.fsyncMode {
	case fsyncModeNever:
		return nil
	case fsyncModeAlways:
		return f.Sync()
	case fsyncModeInterval:
		st.pendingBytes += written
		if st.pendingBytes >= s.flushBytes || time.Since(st.lastFsyncAt) >= s.flushInterval {
			if err := f.Sync(); err != nil {
				return err
			}
			st.pendingBytes = 0
			st.lastFsyncAt = time.Now()
		}
		return nil
	default:
		return f.Sync()
	}
}

// enforceRetentionLocked applies both age and size cleanup policies.
func (s *Storage) enforceRetentionLocked() {
	for topic, parts := range s.partitionState {
		for partition, st := range parts {
			s.applyAgeRetention(topic, partition, st)
			s.applySizeRetention(topic, partition, st)
		}
	}
}

// applyAgeRetention deletes oldest segments past retention age.
func (s *Storage) applyAgeRetention(topic string, partition int, st *partitionState) {
	if s.retentionMaxAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.retentionMaxAge)
	for len(st.segments) > 1 {
		segID := st.segments[0]
		path := s.segmentPath(topic, partition, segID)
		info, err := os.Stat(path)
		if err != nil {
			st.segments = st.segments[1:]
			continue
		}
		if info.ModTime().After(cutoff) {
			break
		}
		s.deleteSegment(topic, partition, st, segID)
	}
}

// applySizeRetention deletes oldest segments until total size is under limit.
func (s *Storage) applySizeRetention(topic string, partition int, st *partitionState) {
	if s.retentionMaxBytes <= 0 {
		return
	}
	total := int64(0)
	for _, segID := range st.segments {
		info, err := os.Stat(s.segmentPath(topic, partition, segID))
		if err == nil {
			total += info.Size()
		}
	}
	for total > s.retentionMaxBytes && len(st.segments) > 1 {
		segID := st.segments[0]
		path := s.segmentPath(topic, partition, segID)
		info, err := os.Stat(path)
		if err == nil {
			total -= info.Size()
		}
		s.deleteSegment(topic, partition, st, segID)
	}
}

// deleteSegment removes one segment and rebuilds in-memory partition state.
func (s *Storage) deleteSegment(topic string, partition int, st *partitionState, segID int) {
	_ = os.Remove(s.segmentPath(topic, partition, segID))
	st.segments = st.segments[1:]
	s.rebuildPartitionFromSegments(topic, partition, st)
}

// rebuildPartitionFromSegments replays remaining segments after retention delete.
func (s *Storage) rebuildPartitionFromSegments(topic string, partition int, st *partitionState) {
	if _, ok := s.data[topic]; ok {
		s.data[topic][partition] = []types.Message{}
	}
	st.nextOffset = 0
	for _, segID := range st.segments {
		s.loadSegment(topic, partition, segID, s.segmentPath(topic, partition, segID))
	}
}

// parseSegmentFileName extracts topic/partition/segment metadata from file name.
func parseSegmentFileName(filename string) (string, int, int, bool) {
	if !strings.HasSuffix(filename, ".log") || !strings.Contains(filename, "-segment-") {
		return "", 0, 0, false
	}
	base := strings.TrimSuffix(filename, ".log")
	parts := strings.Split(base, "-segment-")
	if len(parts) != 2 {
		return "", 0, 0, false
	}
	segID, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, 0, false
	}
	left := parts[0]
	idx := strings.LastIndex(left, "-")
	if idx <= 0 {
		return "", 0, 0, false
	}
	topic := left[:idx]
	partition, err := strconv.Atoi(left[idx+1:])
	if err != nil {
		return "", 0, 0, false
	}
	return topic, partition, segID, true
}

func valueOrDefaultInt64(v int64, fallback int64) int64 {
	if v > 0 {
		return v
	}
	return fallback
}

func valueOrDefaultDuration(v time.Duration, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}

func valueOrDefaultString(v string, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

// DebugChecksum returns checksum in hex for testing and diagnostics.
func DebugChecksum(value string) string {
	out := make([]byte, 4)
	crc := crc32.ChecksumIEEE([]byte(value))
	out[0] = byte(crc >> 24)
	out[1] = byte(crc >> 16)
	out[2] = byte(crc >> 8)
	out[3] = byte(crc)
	return hex.EncodeToString(out)
}
