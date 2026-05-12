package broker

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Broker struct {
	topics map[string][]string
	mutex  sync.Mutex
}

func NewBroker() *Broker {

	b := &Broker{
		topics: make(map[string][]string),
	}

	os.MkdirAll("data", os.ModePerm)

	b.loadData()

	return b
}

func (b *Broker) loadData() {

	files, err := filepath.Glob("data/*.log")
	if err != nil {
		fmt.Println("Error loading data:", err)
		return
	}

	for _, filePath := range files {

		topic := strings.TrimSuffix(
			filepath.Base(filePath),
			".log",
		)

		file, err := os.Open(filePath)
		if err != nil {
			fmt.Println("File open error:", err)
			continue
		}

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {

			line := scanner.Text()

			parts := strings.SplitN(line, ":", 2)

			if len(parts) != 2 {
				continue
			}

			message := parts[1]

			b.topics[topic] = append(
				b.topics[topic],
				message,
			)
		}

		file.Close()
	}

	fmt.Println("Data loaded from disk")
}

func (b *Broker) AddMessage(topic string, message string) int {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	offset := len(b.topics[topic])

	logLine := fmt.Sprintf("%d:%s\n", offset, message)

	filePath := filepath.Join("data", topic+".log")

	file, err := os.OpenFile(
		filePath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		fmt.Println("File open error:", err)
		return -1
	}

	defer file.Close()

	_, err = file.WriteString(logLine)
	if err != nil {
		fmt.Println("File write error:", err)
		return -1
	}

	b.topics[topic] = append(
		b.topics[topic],
		message,
	)

	return offset
}

func (b *Broker) Consume(
	topic string,
	offset int,
) []string {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	messages, exists := b.topics[topic]

	if !exists {
		return []string{}
	}

	if offset >= len(messages) {
		return []string{}
	}

	return messages[offset:]
}

func ParseOffset(value string) int {

	offset, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}

	return offset
}
