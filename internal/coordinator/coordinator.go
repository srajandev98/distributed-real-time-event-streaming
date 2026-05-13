package coordinator

type Coordinator struct {
	GroupManager  *GroupManager
	OffsetManager *OffsetManager
}

func NewCoordinator() *Coordinator {

	return &Coordinator{
		GroupManager:  NewGroupManager(),
		OffsetManager: NewOffsetManager(),
	}
}
