package coordinator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"real-time-event-streaming/internal/logging"
)

// OffsetManager persists committed offsets per group/topic/partition.
type OffsetManager struct {
	dataDir string
	offsets map[string]map[string]map[int]int
	mutex   sync.Mutex
}

// NewOffsetManager creates the manager and loads persisted offsets.
func NewOffsetManager(dataDir string) *OffsetManager {
	om := &OffsetManager{
		dataDir: dataDir,
		offsets: make(map[string]map[string]map[int]int),
	}
	om.load()
	return om
}

// Commit saves consumer progress for one partition.
func (om *OffsetManager) Commit(group string, topic string, partition int, offset int) {
	om.mutex.Lock()
	defer om.mutex.Unlock()

	if _, exists := om.offsets[group]; !exists {
		om.offsets[group] = make(map[string]map[int]int)
	}
	if _, exists := om.offsets[group][topic]; !exists {
		om.offsets[group][topic] = make(map[int]int)
	}

	om.offsets[group][topic][partition] = offset
	om.save()
}

// GetOffset reads last committed progress; returns 0 when missing.
func (om *OffsetManager) GetOffset(group string, topic string, partition int) int {
	om.mutex.Lock()
	defer om.mutex.Unlock()

	groupData, exists := om.offsets[group]
	if !exists {
		return 0
	}
	topicData, exists := groupData[topic]
	if !exists {
		return 0
	}
	offset, exists := topicData[partition]
	if !exists {
		return 0
	}
	return offset
}

// save writes the full offsets map to disk as JSON.
func (om *OffsetManager) save() {
	if err := os.MkdirAll(om.dataDir, 0o755); err != nil {
		logging.Error("offset save mkdir failed", "data_dir", om.dataDir, "error", err)
		return
	}

	path := filepath.Join(om.dataDir, "offsets.json")
	file, err := os.Create(path)
	if err != nil {
		logging.Error("offset save open failed", "path", path, "error", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(om.offsets); err != nil {
		logging.Error("offset save encode failed", "path", path, "error", err)
	}
}

// load restores offsets map from disk if file exists.
func (om *OffsetManager) load() {
	path := filepath.Join(om.dataDir, "offsets.json")
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&om.offsets); err != nil {
		logging.Error("offset load decode failed", "path", path, "error", err)
	}
}
