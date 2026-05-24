package coordinator

import "flux/internal/config"

// Coordinator bundles group and offset management responsibilities.
type Coordinator struct {
	GroupManager  *GroupManager
	OffsetManager *OffsetManager
}

// NewCoordinator wires sub-managers used for consumer coordination.
func NewCoordinator(cfg *config.Config) *Coordinator {
	return &Coordinator{
		GroupManager:  NewGroupManager(cfg.NumPartitions, cfg.DataDir),
		OffsetManager: NewOffsetManager(cfg.DataDir),
	}
}
