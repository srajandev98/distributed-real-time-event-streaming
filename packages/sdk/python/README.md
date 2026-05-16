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

produced = client.produce("orders", "user1", "created")
print("produced", produced)

messages = client.consume("orders", produced.partition, 0)
print("messages", messages)

join = client.join("analytics", "orders", "consumer-a")
print("assigned", join.assigned)

client.commit("analytics", "orders", produced.partition, produced.offset + 1)
print("offset", client.offset("analytics", "orders", produced.partition))

client.close()
```

## API

- `connect() -> None`
- `close() -> None`
- `send_command(command: str, args: str = "") -> RTESResponse`
- `produce(topic: str, key: str, value: str) -> ProduceResult`
- `consume(topic: str, partition: int, offset: int) -> list[ConsumedMessage]`
- `join(group: str, topic: str, consumer_id: str) -> JoinResult`
- `commit(group: str, topic: str, partition: int, offset: int) -> bool`
- `offset(group: str, topic: str, partition: int) -> int`

## Errors

Protocol errors are raised as `RTESProtocolError` with:

- `code` (e.g. `BAD_REQUEST`, `UNKNOWN_COMMAND`)
- `message`
- `response` (full parsed response)
