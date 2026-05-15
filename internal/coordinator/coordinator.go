package coordinator

import "real-time-event-streaming/internal/config"

type Coordinator struct {
	GroupManager  *GroupManager
	OffsetManager *OffsetManager
}

func NewCoordinator(cfg *config.Config) *Coordinator {
	return &Coordinator{
		GroupManager:  NewGroupManager(cfg.NumPartitions),
		OffsetManager: NewOffsetManager(cfg.DataDir),
	}
}
