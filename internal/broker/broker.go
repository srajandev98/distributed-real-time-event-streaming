package broker

import (
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/coordinator"
	"real-time-event-streaming/internal/storage"
)

type Broker struct {
	Storage     *storage.Storage
	Coordinator *coordinator.Coordinator
}

func NewBroker(cfg *config.Config) *Broker {
	return &Broker{
		Storage:     storage.NewStorage(cfg),
		Coordinator: coordinator.NewCoordinator(cfg),
	}
}
