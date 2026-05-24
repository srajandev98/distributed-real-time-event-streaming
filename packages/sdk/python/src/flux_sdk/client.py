from __future__ import annotations

import socket
from dataclasses import dataclass
from threading import Lock
from typing import Optional

PROTOCOL_VERSION = "V1"


@dataclass
class FLUXResponse:
    version: str
    correlation_id: str
    status: str
    payload: Optional[str] = None
    code: Optional[str] = None
    message: Optional[str] = None
    raw: str = ""


@dataclass
class ProduceResult:
    partition: int
    offset: int
    high_watermark: Optional[int]
    raw: FLUXResponse


@dataclass
class ConsumedMessage:
    offset: int
    value: str


@dataclass
class JoinResult:
    generation: int
    assigned: list[int]
    raw: FLUXResponse

@dataclass
class SyncResult:
    generation: int
    assigned: list[int]
    raw: FLUXResponse

@dataclass
class ReplicaFetchResult:
    replica_id: int
    acked_offset: int
    high_watermark: int
    isr: list[int]
    under_replicated: bool
    raw: FLUXResponse

@dataclass
class PartitionRoleResult:
    topic: str
    partition: int
    role: str
    high_watermark: int
    raw: FLUXResponse

@dataclass
class AdminCreateTopicResult:
    topic: str
    partitions: int
    replication_factor: int
    term: int
    index: int
    raw: FLUXResponse


@dataclass
class AdminRegisterBrokerResult:
    broker_id: int
    host: str
    port: int
    term: int
    index: int
    raw: FLUXResponse


@dataclass
class AdminBrokerHeartbeatResult:
    broker_id: int
    heartbeat_ok: bool
    term: int
    index: int
    raw: FLUXResponse


@dataclass
class AdminSetPartitionLeaderResult:
    topic: str
    partition: int
    leader: int
    isr: list[int]
    term: int
    index: int
    raw: FLUXResponse


@dataclass
class AdminMetadataResult:
    topics: list[str]
    brokers: list[int]
    raw: FLUXResponse


class FLUXProtocolError(Exception):
    def __init__(self, message: str, code: str, response: FLUXResponse):
        super().__init__(message)
        self.code = code
        self.response = response


class FLUXClient:
    """Simple Python client for FLUX line-based TCP protocol."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9092, timeout_seconds: float = 5.0):
        self.host = host
        self.port = port
        self.timeout_seconds = timeout_seconds

        self._sock: Optional[socket.socket] = None
        self._file = None
        self._correlation_seq = 1
        self._lock = Lock()

    def connect(self) -> None:
        if self._sock is not None:
            return

        sock = socket.create_connection((self.host, self.port), timeout=self.timeout_seconds)
        sock.settimeout(self.timeout_seconds)
        self._sock = sock
        self._file = sock.makefile("r", encoding="utf-8", newline="\n")

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def produce(self, topic: str, key: str, value: str, acks: str = "1") -> ProduceResult:
        if acks not in ("0", "1", "all"):
            raise ValueError("acks must be one of: '0', '1', 'all'")
        resp = self.send_command("PRODUCE", f"{topic} {key}:{value} acks={acks}")
        payload = self._must_payload(resp, "produce response payload missing")
        hw: Optional[int] = None
        try:
            hw = self._extract_int_field(payload, "hw")
        except ValueError:
            hw = None
        return ProduceResult(
            partition=self._extract_int_field(payload, "partition"),
            offset=self._extract_int_field(payload, "offset"),
            high_watermark=hw,
            raw=resp,
        )

    def consume(self, topic: str, partition: int, offset: int) -> list[ConsumedMessage]:
        resp = self.send_command("CONSUME", f"{topic} {partition} {offset}")
        payload = self._must_payload(resp, "consume response payload missing")

        parts = payload.split("messages=", 1)
        data = parts[1] if len(parts) == 2 else ""
        data = data.split(" hw=", 1)[0].strip()
        if data.strip() == "":
            return []

        out: list[ConsumedMessage] = []
        for item in data.split(","):
            if item == "":
                continue
            segs = item.split(":")
            if len(segs) < 2:
                raise ValueError(f"Invalid consume message item: {item}")
            msg_offset = int(segs[0])
            msg_value = ":".join(segs[1:])
            out.append(ConsumedMessage(offset=msg_offset, value=msg_value))

        return out

    def join(self, group: str, topic: str, consumer_id: str, assignor: Optional[str] = None) -> JoinResult:
        if assignor is not None and assignor not in ("round_robin", "range"):
            raise ValueError("assignor must be one of: 'round_robin', 'range'")
        suffix = f" assignor={assignor}" if assignor is not None else ""
        resp = self.send_command("JOIN", f"{group} {topic} {consumer_id}{suffix}")
        payload = self._must_payload(resp, "join response payload missing")
        generation = self._extract_int_field(payload, "generation")

        start = payload.find("[")
        end = payload.find("]")
        if start == -1 or end == -1 or end <= start:
            return JoinResult(generation=generation, assigned=[], raw=resp)

        body = payload[start + 1 : end].strip()
        if body == "":
            return JoinResult(generation=generation, assigned=[], raw=resp)

        assigned = [int(v) for v in body.split()]
        return JoinResult(generation=generation, assigned=assigned, raw=resp)

    def sync(self, group: str, topic: str, consumer_id: str, generation: int) -> SyncResult:
        resp = self.send_command("SYNC", f"{group} {topic} {consumer_id} {generation}")
        payload = self._must_payload(resp, "sync response payload missing")
        parsed_generation = self._extract_int_field(payload, "generation")

        start = payload.find("[")
        end = payload.find("]")
        if start == -1 or end == -1 or end <= start:
            return SyncResult(generation=parsed_generation, assigned=[], raw=resp)
        body = payload[start + 1 : end].strip()
        if body == "":
            return SyncResult(generation=parsed_generation, assigned=[], raw=resp)
        assigned = [int(v) for v in body.split()]
        return SyncResult(generation=parsed_generation, assigned=assigned, raw=resp)

    def heartbeat(self, group: str, topic: str, consumer_id: str, generation: int) -> bool:
        resp = self.send_command("HEARTBEAT", f"{group} {topic} {consumer_id} {generation}")
        payload = self._must_payload(resp, "heartbeat response payload missing")
        return "heartbeat=ok" in payload

    def leave(self, group: str, topic: str, consumer_id: str, generation: int) -> bool:
        resp = self.send_command("LEAVE", f"{group} {topic} {consumer_id} {generation}")
        payload = self._must_payload(resp, "leave response payload missing")
        return "left=true" in payload

    def commit(
        self,
        group: str,
        topic: str,
        consumer_id: str,
        generation: int,
        partition: int,
        offset: int,
    ) -> bool:
        resp = self.send_command("COMMIT", f"{group} {topic} {consumer_id} {generation} {partition} {offset}")
        payload = self._must_payload(resp, "commit response payload missing")
        return "committed=true" in payload

    def offset(self, group: str, topic: str, partition: int) -> int:
        resp = self.send_command("OFFSET", f"{group} {topic} {partition}")
        payload = self._must_payload(resp, "offset response payload missing")
        return self._extract_int_field(payload, "offset")

    def replica_fetch(self, topic: str, partition: int, replica_id: int, offset: int) -> ReplicaFetchResult:
        resp = self.send_command("REPLICA_FETCH", f"{topic} {partition} {replica_id} {offset}")
        payload = self._must_payload(resp, "replica fetch response payload missing")
        return ReplicaFetchResult(
            replica_id=self._extract_int_field(payload, "replica"),
            acked_offset=self._extract_int_field(payload, "acked_offset"),
            high_watermark=self._extract_int_field(payload, "hw"),
            isr=self._extract_int_list_field(payload, "isr"),
            under_replicated=self._extract_bool_field(payload, "under_replicated"),
            raw=resp,
        )

    def set_partition_role(self, topic: str, partition: int, role: str) -> PartitionRoleResult:
        if role not in ("leader", "follower"):
            raise ValueError("role must be one of: 'leader', 'follower'")
        resp = self.send_command("SET_PARTITION_ROLE", f"{topic} {partition} {role}")
        payload = self._must_payload(resp, "set partition role payload missing")
        parsed_role = self._extract_string_field(payload, "role")
        if parsed_role not in ("leader", "follower"):
            raise ValueError(f"Invalid role in payload: {payload}")
        return PartitionRoleResult(
            topic=self._extract_string_field(payload, "topic"),
            partition=self._extract_int_field(payload, "partition"),
            role=parsed_role,
            high_watermark=self._extract_int_field(payload, "hw"),
            raw=resp,
        )

    def admin_create_topic(self, topic: str, partitions: int, replication_factor: int) -> AdminCreateTopicResult:
        resp = self.send_command("ADMIN_CREATE_TOPIC", f"{topic} {partitions} {replication_factor}")
        payload = self._must_payload(resp, "admin create topic payload missing")
        return AdminCreateTopicResult(
            topic=self._extract_string_field(payload, "topic"),
            partitions=self._extract_int_field(payload, "partitions"),
            replication_factor=self._extract_int_field(payload, "replication_factor"),
            term=self._extract_int_field(payload, "term"),
            index=self._extract_int_field(payload, "index"),
            raw=resp,
        )

    def admin_register_broker(self, broker_id: int, host: str, port: int, epoch: Optional[int] = None) -> AdminRegisterBrokerResult:
        suffix = f" {epoch}" if epoch is not None else ""
        resp = self.send_command("ADMIN_REGISTER_BROKER", f"{broker_id} {host} {port}{suffix}")
        payload = self._must_payload(resp, "admin register broker payload missing")
        return AdminRegisterBrokerResult(
            broker_id=self._extract_int_field(payload, "broker_id"),
            host=self._extract_string_field(payload, "host"),
            port=self._extract_int_field(payload, "port"),
            term=self._extract_int_field(payload, "term"),
            index=self._extract_int_field(payload, "index"),
            raw=resp,
        )

    def admin_broker_heartbeat(self, broker_id: int) -> AdminBrokerHeartbeatResult:
        resp = self.send_command("ADMIN_BROKER_HEARTBEAT", f"{broker_id}")
        payload = self._must_payload(resp, "admin broker heartbeat payload missing")
        return AdminBrokerHeartbeatResult(
            broker_id=self._extract_int_field(payload, "broker_id"),
            heartbeat_ok="heartbeat=ok" in payload,
            term=self._extract_int_field(payload, "term"),
            index=self._extract_int_field(payload, "index"),
            raw=resp,
        )

    def admin_set_partition_leader(self, topic: str, partition: int, leader_id: int, isr: list[int]) -> AdminSetPartitionLeaderResult:
        isr_csv = ",".join(str(v) for v in isr)
        resp = self.send_command("ADMIN_SET_PARTITION_LEADER", f"{topic} {partition} {leader_id} {isr_csv}")
        payload = self._must_payload(resp, "admin set partition leader payload missing")
        return AdminSetPartitionLeaderResult(
            topic=self._extract_string_field(payload, "topic"),
            partition=self._extract_int_field(payload, "partition"),
            leader=self._extract_int_field(payload, "leader"),
            isr=self._extract_int_list_field(payload, "isr"),
            term=self._extract_int_field(payload, "term"),
            index=self._extract_int_field(payload, "index"),
            raw=resp,
        )

    def admin_get_metadata(self) -> AdminMetadataResult:
        resp = self.send_command("ADMIN_GET_METADATA")
        payload = self._must_payload(resp, "admin get metadata payload missing")
        topics = self._extract_list_field(payload, "topics")
        brokers = [int(v) for v in self._extract_list_field(payload, "brokers") if v != ""]
        return AdminMetadataResult(topics=topics, brokers=brokers, raw=resp)

    def send_command(self, command: str, args: str = "") -> FLUXResponse:
        if self._sock is None or self._file is None:
            raise RuntimeError("FLUX client is not connected. Call connect() first.")

        with self._lock:
            correlation_id = str(self._correlation_seq)
            self._correlation_seq += 1

            line = f"{PROTOCOL_VERSION}|{correlation_id}|{command}|{args}\n"
            self._sock.sendall(line.encode("utf-8"))

            raw_line = self._file.readline()
            if raw_line == "":
                raise ConnectionError("FLUX connection closed by server")

            response = self._parse_response(raw_line.strip())
            if response.correlation_id != correlation_id:
                raise RuntimeError(
                    f"Correlation mismatch: expected={correlation_id} got={response.correlation_id}"
                )

            if response.status == "ERR":
                raise FLUXProtocolError(
                    response.message or "FLUX protocol error",
                    response.code or "UNKNOWN_ERROR",
                    response,
                )

            return response

    def _parse_response(self, line: str) -> FLUXResponse:
        parts = line.split("|")
        if len(parts) < 3:
            raise ValueError(f"Invalid FLUX response: {line}")

        version, correlation_id, status = parts[0], parts[1], parts[2]
        if version != PROTOCOL_VERSION:
            raise ValueError(f"Unsupported FLUX response version: {version}")

        if status == "OK":
            payload = "|".join(parts[3:]) if len(parts) > 3 else ""
            return FLUXResponse(
                version=version,
                correlation_id=correlation_id,
                status="OK",
                payload=payload,
                raw=line,
            )

        if status == "ERR":
            code = parts[3] if len(parts) > 3 else "UNKNOWN_ERROR"
            message = "|".join(parts[4:]) if len(parts) > 4 else "unknown error"
            return FLUXResponse(
                version=version,
                correlation_id=correlation_id,
                status="ERR",
                code=code,
                message=message,
                raw=line,
            )

        raise ValueError(f"Unknown FLUX response status: {status}")

    @staticmethod
    def _must_payload(resp: FLUXResponse, fallback: str) -> str:
        if resp.payload is None:
            raise ValueError(fallback)
        return resp.payload

    @staticmethod
    def _extract_int_field(payload: str, key: str) -> int:
        token = f"{key}="
        start = payload.find(token)
        if start == -1:
            raise ValueError(f"Field {key} missing in payload: {payload}")

        num = payload[start + len(token) :].split()[0]
        return int(num)

    @staticmethod
    def _extract_int_list_field(payload: str, key: str) -> list[int]:
        token = f"{key}=["
        start = payload.find(token)
        if start == -1:
            return []

        rest = payload[start + len(token) :]
        end = rest.find("]")
        if end == -1:
            raise ValueError(f"Field {key} malformed in payload: {payload}")

        body = rest[:end].strip()
        if body == "":
            return []

        return [int(v) for v in body.split()]

    @staticmethod
    def _extract_bool_field(payload: str, key: str) -> bool:
        token = f"{key}="
        start = payload.find(token)
        if start == -1:
            raise ValueError(f"Field {key} missing in payload: {payload}")

        value = payload[start + len(token) :].split()[0].strip().lower()
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"Invalid boolean for {key}: {payload}")

    @staticmethod
    def _extract_string_field(payload: str, key: str) -> str:
        token = f"{key}="
        start = payload.find(token)
        if start == -1:
            raise ValueError(f"Field {key} missing in payload: {payload}")
        return payload[start + len(token) :].split()[0].strip()

    @staticmethod
    def _extract_list_field(payload: str, key: str) -> list[str]:
        token = f"{key}=["
        start = payload.find(token)
        if start == -1:
            return []
        rest = payload[start + len(token) :]
        end = rest.find("]")
        if end == -1:
            raise ValueError(f"Field {key} malformed in payload: {payload}")
        body = rest[:end].strip()
        if body == "":
            return []
        return [v for v in body.split(" ") if v != ""]
