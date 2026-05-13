package broker

import (
	"real-time-event-streaming/internal/coordinator"
	"real-time-event-streaming/internal/storage"
)

type Broker struct {
	Storage     *storage.Storage
	Coordinator *coordinator.Coordinator
}

func NewBroker() *Broker {

	return &Broker{
		Storage:     storage.NewStorage(),
		Coordinator: coordinator.NewCoordinator(),
	}
}
