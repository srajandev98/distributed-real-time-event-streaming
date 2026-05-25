# FLUX Python SDK

Python client SDK for Real-time Event Streaming (FLUX).

## Install (local path)

```bash
pip install ../../packages/sdk/python
```

Or publish this package and install by package name.

## Usage

```python
from flux_sdk import FLUXClient

client = FLUXClient(host="127.0.0.1", port=9092)
client.connect()

produced = client.produce("orders", "user1", "created", acks="all")
print("produced", produced)

messages = client.consume("orders", produced.partition, 0)
print("messages", messages)

join = client.join("analytics", "orders", "consumer-a", assignor="round_robin")
print("generation", join.generation)
print("assigned", join.assigned)

print("heartbeat", client.heartbeat("analytics", "orders", "consumer-a", join.generation))
print("sync", client.sync("analytics", "orders", "consumer-a", join.generation))

client.commit("analytics", "orders", "consumer-a", join.generation, produced.partition, produced.offset + 1)
print("offset", client.offset("analytics", "orders", produced.partition))
print("replica", client.replica_fetch("orders", produced.partition, 1, produced.offset))
print("role", client.set_partition_role("orders", produced.partition, "leader"))
print("left", client.leave("analytics", "orders", "consumer-a", join.generation))

client.close()
```

## High-level runtime API

```python
from flux_sdk import FLUXRuntime, ProducerMessage

runtime = FLUXRuntime(host="127.0.0.1", port=9092)

producer = runtime.producer(max_retries=3, retry_backoff_ms=250, batch_size=100)
producer.send(
    topic="orders",
    messages=[
        ProducerMessage(key="user1", value="created"),
        ProducerMessage(key="user2", value="paid"),
    ],
    acks="all",
)

consumer = runtime.consumer(
    group_id="analytics",
    consumer_id="consumer-a",
    assignor="round_robin",
    on_assign=lambda partitions: print("assigned", partitions),
    on_revoke=lambda partitions: print("revoked", partitions),
    on_crash=lambda err: print("consumer crashed", err),
)
consumer.subscribe("orders")

def handle_message(ctx):
    print("topic=", ctx.topic, "partition=", ctx.partition, "offset=", ctx.message.offset, "value=", ctx.message.value)
    # Call consumer.disconnect() when your app wants to stop the run loop.

consumer.run(handle_message)
```

## Runtime failover (multi-broker)

```python
from flux_sdk import FLUXRuntime

runtime = FLUXRuntime(
    brokers=[
        ("127.0.0.1", 9092),
        ("127.0.0.1", 9093),
        ("127.0.0.1", 9094),
    ]
)
```

When enabled, runtime producer sends automatically rotate to the next broker on
`NOT_LEADER` and common transient connection failures.

## Admin APIs (Operator Flow)

Admin APIs are intended for platform/operator workflows, not typical app producer/consumer code paths.

```python
from flux_sdk import FLUXClient

client = FLUXClient(host="127.0.0.1", port=9092)
client.connect()

print(client.admin_create_topic("payments", 3, 2))
print(client.admin_register_broker(1, "127.0.0.1", 9093))
print(client.admin_broker_heartbeat(1))
print(client.admin_set_partition_leader("payments", 0, 1, [1, 0]))
print(client.admin_get_metadata())

client.close()
```

## API

- `connect() -> None`
- `close() -> None`
- `send_command(command: str, args: str = "") -> FLUXResponse`
- `produce(topic: str, key: str, value: str, acks: str = "1") -> ProduceResult`
- `consume(topic: str, partition: int, offset: int) -> list[ConsumedMessage]`
- `join(group: str, topic: str, consumer_id: str, assignor: Optional[str] = None) -> JoinResult`
- `sync(group: str, topic: str, consumer_id: str, generation: int) -> SyncResult`
- `heartbeat(group: str, topic: str, consumer_id: str, generation: int) -> bool`
- `leave(group: str, topic: str, consumer_id: str, generation: int) -> bool`
- `commit(group: str, topic: str, consumer_id: str, generation: int, partition: int, offset: int) -> bool`
- `offset(group: str, topic: str, partition: int) -> int`
- `replica_fetch(topic: str, partition: int, replica_id: int, offset: int) -> ReplicaFetchResult`
- `set_partition_role(topic: str, partition: int, role: str) -> PartitionRoleResult`
- `admin_create_topic(topic: str, partitions: int, replication_factor: int) -> AdminCreateTopicResult`
- `admin_register_broker(broker_id: int, host: str, port: int, epoch: Optional[int] = None) -> AdminRegisterBrokerResult`
- `admin_broker_heartbeat(broker_id: int) -> AdminBrokerHeartbeatResult`
- `admin_set_partition_leader(topic: str, partition: int, leader_id: int, isr: list[int]) -> AdminSetPartitionLeaderResult`
- `admin_get_metadata() -> AdminMetadataResult`
- `FLUXRuntime.producer(...) -> FLUXProducer`
- `FLUXProducer.send(topic: str, messages: list[ProducerMessage], acks: str = "1") -> None`
- `FLUXRuntime.consumer(...) -> FLUXConsumer`
- `FLUXConsumer.subscribe(topic: str) -> None`
- `FLUXConsumer.run(each_message: Callable[[ConsumerRunContext], None]) -> None`
- `FLUXConsumer.disconnect() -> None`

## Errors

Protocol errors are raised as `FLUXProtocolError` with:

- `code` (e.g. `BAD_REQUEST`, `UNKNOWN_COMMAND`)
- `message`
- `response` (full parsed response)
