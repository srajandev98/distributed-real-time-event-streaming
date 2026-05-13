package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type OffsetManager struct {
	offsets map[string]map[string]map[int]int
	mutex   sync.Mutex
}

func NewOffsetManager() *OffsetManager {

	om := &OffsetManager{
		offsets: make(
			map[string]map[string]map[int]int,
		),
	}

	om.load()

	return om
}

func (om *OffsetManager) Commit(
	group string,
	topic string,
	partition int,
	offset int,
) {

	om.mutex.Lock()
	defer om.mutex.Unlock()

	if _, exists := om.offsets[group]; !exists {

		om.offsets[group] = make(
			map[string]map[int]int,
		)
	}

	if _, exists := om.offsets[group][topic]; !exists {

		om.offsets[group][topic] =
			make(map[int]int)
	}

	om.offsets[group][topic][partition] =
		offset

	om.save()
}

func (om *OffsetManager) GetOffset(
	group string,
	topic string,
	partition int,
) int {

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

	file, err := os.Create(
		"data/offsets.json",
	)

	if err != nil {
		fmt.Println(
			"Offset save error:",
			err,
		)

		return
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(om.offsets)

	if err != nil {

		fmt.Println(
			"JSON encode error:",
			err,
		)
	}
}

func (om *OffsetManager) load() {

	file, err := os.Open(
		"data/offsets.json",
	)

	if err != nil {
		return
	}

	defer file.Close()

	decoder := json.NewDecoder(file)

	err = decoder.Decode(&om.offsets)

	if err != nil {

		fmt.Println(
			"Offset load error:",
			err,
		)
	}
}
