package controlplane

import "fmt"

// CommandType represents a metadata mutation event to replicate via consensus.
type CommandType string

const (
	CommandUpsertTopic        CommandType = "UPSERT_TOPIC"
	CommandRegisterBroker     CommandType = "REGISTER_BROKER"
	CommandBrokerHeartbeat    CommandType = "BROKER_HEARTBEAT"
	CommandSetPartitionLeader CommandType = "SET_PARTITION_LEADER"
)

// ApplyResult captures the logical consensus position after applying a command.
type ApplyResult struct {
	Term  uint64
	Index uint64
}

// Command is the mutation payload for metadata state machine transitions.
type Command struct {
	Type CommandType

	Topic           *TopicMetadata
	Broker          *BrokerMetadata
	BrokerHeartbeat *BrokerHeartbeat
	PartitionLeader *PartitionLeaderUpdate
}

// BrokerHeartbeat updates broker liveness metadata.
type BrokerHeartbeat struct {
	BrokerID int
}

// PartitionLeaderUpdate updates partition leader and ISR seed.
type PartitionLeaderUpdate struct {
	Topic     string
	Partition int
	LeaderID  int
	ISR       []int
}

// MetadataStore abstracts consensus-backed metadata persistence.
type MetadataStore interface {
	Apply(cmd Command) (ApplyResult, error)
	Snapshot() MetadataSnapshot
}

func validateCommand(cmd Command) error {
	switch cmd.Type {
	case CommandUpsertTopic:
		if cmd.Topic == nil || cmd.Topic.Name == "" {
			return fmt.Errorf("invalid upsert topic command")
		}
	case CommandRegisterBroker:
		if cmd.Broker == nil || cmd.Broker.ID < 0 {
			return fmt.Errorf("invalid register broker command")
		}
	case CommandBrokerHeartbeat:
		if cmd.BrokerHeartbeat == nil || cmd.BrokerHeartbeat.BrokerID < 0 {
			return fmt.Errorf("invalid broker heartbeat command")
		}
	case CommandSetPartitionLeader:
		if cmd.PartitionLeader == nil || cmd.PartitionLeader.Topic == "" || cmd.PartitionLeader.Partition < 0 {
			return fmt.Errorf("invalid set partition leader command")
		}
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
	return nil
}
