package coordinator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type OffsetManager struct {
	dataDir string
	offsets map[string]map[string]map[int]int
	mutex   sync.Mutex
}

func NewOffsetManager(dataDir string) *OffsetManager {
	om := &OffsetManager{
		dataDir: dataDir,
		offsets: make(map[string]map[string]map[int]int),
	}
	om.load()
	return om
}

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

func (om *OffsetManager) save() {
	if err := os.MkdirAll(om.dataDir, 0o755); err != nil {
		fmt.Println("Offset save mkdir error:", err)
		return
	}

	path := filepath.Join(om.dataDir, "offsets.json")
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("Offset save error:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(om.offsets); err != nil {
		fmt.Println("JSON encode error:", err)
	}
}

func (om *OffsetManager) load() {
	path := filepath.Join(om.dataDir, "offsets.json")
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&om.offsets); err != nil {
		fmt.Println("Offset load error:", err)
	}
}
