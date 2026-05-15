package storage

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/types"
)

type Storage struct {
	data          map[string]map[int][]types.Message
	dataDir       string
	numPartitions int
	mutex         sync.Mutex
}

func NewStorage(cfg *config.Config) *Storage {
	s := &Storage{
		data:          make(map[string]map[int][]types.Message),
		dataDir:       cfg.DataDir,
		numPartitions: cfg.NumPartitions,
	}

	s.loadData()
	return s
}

func (s *Storage) Produce(topic string, key string, value string) (int, int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	partition := getPartition(key, s.numPartitions)
	if _, exists := s.data[topic]; !exists {
		s.data[topic] = make(map[int][]types.Message)
	}

	offset := len(s.data[topic][partition])
	message := types.Message{Offset: offset, Value: value}
	s.data[topic][partition] = append(s.data[topic][partition], message)

	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		panic(err)
	}

	fileName := fmt.Sprintf("%s-%d.log", topic, partition)
	path := filepath.Join(s.dataDir, fileName)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	logLine := fmt.Sprintf("%d:%s\n", offset, value)
	if _, err := file.WriteString(logLine); err != nil {
		panic(err)
	}

	return partition, offset
}

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

func getPartition(key string, numPartitions int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32()) % numPartitions
}

func (s *Storage) loadData() {
	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		return
	}

	for _, file := range files {
		filename := file.Name()
		if strings.Contains(filename, "replica") {
			continue
		}
		if !strings.HasSuffix(filename, ".log") {
			continue
		}

		parts := strings.Split(filename, "-")
		if len(parts) != 2 {
			continue
		}

		topic := parts[0]
		partitionPart := strings.TrimSuffix(parts[1], ".log")
		partition, err := strconv.Atoi(partitionPart)
		if err != nil {
			continue
		}

		path := filepath.Join(s.dataDir, filename)
		f, err := os.Open(path)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			lineParts := strings.SplitN(line, ":", 2)
			if len(lineParts) != 2 {
				continue
			}

			offset, err := strconv.Atoi(lineParts[0])
			if err != nil {
				continue
			}

			value := lineParts[1]
			if _, exists := s.data[topic]; !exists {
				s.data[topic] = make(map[int][]types.Message)
			}

			message := types.Message{Offset: offset, Value: value}
			s.data[topic][partition] = append(s.data[topic][partition], message)
		}

		f.Close()
	}
}
