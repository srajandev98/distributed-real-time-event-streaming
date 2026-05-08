package broker

import (
	"fmt"
	"sync"
)

type Broker struct {
	topics map[string][]string
	mutex  sync.Mutex
}

func NewBroker() *Broker {

	return &Broker{
		topics: make(map[string][]string),
	}
}

func (b *Broker) AddMessage(topic string, message string) {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.topics[topic] = append(b.topics[topic], message)

	fmt.Println("Current broker state:")

	for topicName, messages := range b.topics {
		fmt.Println("Topic:", topicName)

		for index, msg := range messages {
			fmt.Println(index, ":", msg)
		}
	}
}
