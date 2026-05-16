# Class Diagram

This diagram shows the main runtime structures and how requests move through the system.

```mermaid
classDiagram
    class Main {
      +main()
    }

    class Config {
      +ListenAddr string
      +DataDir string
      +NumPartitions int
      +Load() *Config
    }

    class Broker {
      +Storage *Storage
      +Coordinator *Coordinator
      +NewBroker(cfg *Config) *Broker
    }

    class Storage {
      -data map[string]map[int][]Message
      -dataDir string
      -numPartitions int
      +Produce(topic, key, value) (partition, offset)
      +Consume(topic, partition, offset) []Message
      -loadData()
    }

    class Message {
      +Offset int
      +Value string
    }

    class Coordinator {
      +GroupManager *GroupManager
      +OffsetManager *OffsetManager
      +NewCoordinator(cfg *Config) *Coordinator
    }

    class GroupManager {
      -numPartitions int
      -groups map[string]*ConsumerGroup
      +JoinGroup(groupName, topic, consumerID) []int
      -rebalance(group *ConsumerGroup)
    }

    class ConsumerGroup {
      -name string
      -topic string
      -consumers []string
      -assignments map[string][]int
    }

    class OffsetManager {
      -dataDir string
      -offsets map[string]map[string]map[int]int
      +Commit(group, topic, partition, offset)
      +GetOffset(group, topic, partition) int
      -save()
      -load()
    }

    class Request {
      +Version string
      +CorrelationID string
      +Command string
      +Args []string
    }

    class Response {
      +Version string
      +CorrelationID string
      +Status string
      +Code string
      +Message string
      +Payload string
    }

    class Protocol {
      +ParseRequest(line) (*Request, error)
      +Ok(correlationID, payload) string
      +Err(correlationID, code, message) string
      +ParseInt(value, field) (int, error)
    }

    class NetworkHandler {
      +HandleConnection(conn, broker)
      -handleRequest(req, broker) string
      -handleProduce(req, broker) string
      -handleConsume(req, broker) string
      -handleJoin(req, broker) string
      -handleCommit(req, broker) string
      -handleOffset(req, broker) string
    }

    class Logging {
      +Info(msg, ...args)
      +Warn(msg, ...args)
      +Error(msg, ...args)
    }

    Main --> Config : loads
    Main --> Broker : creates
    Main --> NetworkHandler : accepts TCP and delegates

    Broker --> Storage : owns
    Broker --> Coordinator : owns

    Coordinator --> GroupManager : owns
    Coordinator --> OffsetManager : owns
    GroupManager --> ConsumerGroup : manages

    NetworkHandler --> Protocol : parse/format
    NetworkHandler --> Broker : execute commands
    Storage --> Message : stores

    Main --> Logging : runtime logs
    NetworkHandler --> Logging : request logs
    GroupManager --> Logging : rebalance logs
    OffsetManager --> Logging : persistence logs
```

## How To Read It Quickly

1. `Main` boots `Config`, builds `Broker`, and starts TCP handling.
2. `NetworkHandler` is the request entry point.
3. `Protocol` validates/parses input before business logic runs.
4. `Broker` routes work to:
   - `Storage` for produce/consume
   - `Coordinator` for group membership and offsets
5. `Coordinator` splits responsibilities into:
   - `GroupManager` (partition assignment)
   - `OffsetManager` (persisted consumer progress)
