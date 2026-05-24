# RTES Python SDK

Python client SDK for Real-time Event Streaming (RTES).

## Install (local path)

```bash
pip install ../../packages/sdk/python
```

Or publish this package and install by package name.

## Usage

```python
from rtes_sdk import RTESClient

client = RTESClient(host="127.0.0.1", port=9092)
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

## High-level high-level API

```python
from rtes_sdk import RTESRuntime, ProducerMessage

runtime = RTESRuntime(host="127.0.0.1", port=9092)

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

## API

- `connect() -> None`
- `close() -> None`
- `send_command(command: str, args: str = "") -> RTESResponse`
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
- `RTESRuntime.producer(...) -> RTESProducer`
- `RTESProducer.send(topic: str, messages: list[ProducerMessage], acks: str = "1") -> None`
- `RTESRuntime.consumer(...) -> RTESConsumer`
- `RTESConsumer.subscribe(topic: str) -> None`
- `RTESConsumer.run(each_message: Callable[[ConsumerRunContext], None]) -> None`
- `RTESConsumer.disconnect() -> None`

## Errors

Protocol errors are raised as `RTESProtocolError` with:

- `code` (e.g. `BAD_REQUEST`, `UNKNOWN_COMMAND`)
- `message`
- `response` (full parsed response)
