package broker

import (
	"real-time-event-streaming/internal/config"
	"real-time-event-streaming/internal/coordinator"
	"real-time-event-streaming/internal/storage"
)

// Broker is the composition root for runtime services used by request handlers.
type Broker struct {
	Storage     *storage.Storage
	Coordinator *coordinator.Coordinator
}

// NewBroker wires storage + coordinator using shared runtime config.
func NewBroker(cfg *config.Config) *Broker {
	return &Broker{
		Storage:     storage.NewStorage(cfg),
		Coordinator: coordinator.NewCoordinator(cfg),
	}
}
