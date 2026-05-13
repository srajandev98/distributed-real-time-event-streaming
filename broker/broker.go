package broker

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const NumPartitions = 10
const ReplicationFactor = 3

type Broker struct {
	topics map[string]map[int][]string
	mutex  sync.Mutex
}

func NewBroker() *Broker {

	b := &Broker{
		topics: make(map[string]map[int][]string),
	}

	os.MkdirAll("data", os.ModePerm)

	b.loadData()

	return b
}

func (b *Broker) loadData() {

	files, err := filepath.Glob("data/*.log")
	if err != nil {
		fmt.Println("Load error:", err)
		return
	}

	for _, filePath := range files {

		filename := filepath.Base(filePath)

		filename = strings.TrimSuffix(
			filename,
			".log",
		)

		if strings.Contains(
			filename,
			"replica",
		) {

			continue
		}

		parts := strings.Split(
			filename,
			"-",
		)

		if len(parts) != 2 {
			continue
		}

		topic := parts[0]

		partition, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		if _, exists := b.topics[topic]; !exists {
			b.topics[topic] = make(map[int][]string)
		}

		file, err := os.Open(filePath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {

			line := scanner.Text()

			lineParts := strings.SplitN(
				line,
				":",
				2,
			)

			if len(lineParts) != 2 {
				continue
			}

			message := lineParts[1]

			b.topics[topic][partition] = append(
				b.topics[topic][partition],
				message,
			)
		}

		file.Close()
	}

	fmt.Println("Recovered logs from disk")
}

func hashKey(key string) int {

	hasher := fnv.New32a()

	hasher.Write([]byte(key))

	hashValue := hasher.Sum32()

	return int(hashValue % NumPartitions)
}

func (b *Broker) Produce(
	topic string,
	key string,
	message string,
) (int, int) {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	partition := hashKey(key)

	if _, exists := b.topics[topic]; !exists {
		b.topics[topic] = make(map[int][]string)
	}

	offset := len(
		b.topics[topic][partition],
	)

	logLine := fmt.Sprintf(
		"%d:%s\n",
		offset,
		message,
	)

	fileName := fmt.Sprintf(
		"%s-%d.log",
		topic,
		partition,
	)

	filePath := filepath.Join(
		"data",
		fileName,
	)

	file, err := os.OpenFile(
		filePath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		fmt.Println("File error:", err)
		return -1, -1
	}

	defer file.Close()

	_, err = file.WriteString(logLine)
	if err != nil {
		fmt.Println("Write error:", err)
		return -1, -1
	}

	for replica := 1; replica < ReplicationFactor; replica++ {

		replicaFileName := fmt.Sprintf(
			"%s-%d-replica-%d.log",
			topic,
			partition,
			replica,
		)

		replicaPath := filepath.Join(
			"data",
			replicaFileName,
		)

		writeReplica(
			replicaPath,
			logLine,
		)
	}

	b.topics[topic][partition] = append(
		b.topics[topic][partition],
		message,
	)

	return partition, offset
}

func writeReplica(
	filePath string,
	logLine string,
) {

	file, err := os.OpenFile(
		filePath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {

		fmt.Println(
			"Replica file error:",
			err,
		)

		return
	}

	defer file.Close()

	_, err = file.WriteString(logLine)

	if err != nil {

		fmt.Println(
			"Replica write error:",
			err,
		)
	}
}

func (b *Broker) Consume(
	topic string,
	partition int,
	offset int,
) []string {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	topicPartitions, exists := b.topics[topic]

	if !exists {
		return []string{}
	}

	messages := topicPartitions[partition]

	if offset >= len(messages) {
		return []string{}
	}

	return messages[offset:]
}
