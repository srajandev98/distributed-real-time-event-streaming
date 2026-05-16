from __future__ import annotations

import socket
from dataclasses import dataclass
from threading import Lock
from typing import Optional

PROTOCOL_VERSION = "V1"


@dataclass
class RTESResponse:
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
    raw: RTESResponse


@dataclass
class ConsumedMessage:
    offset: int
    value: str


@dataclass
class JoinResult:
    assigned: list[int]
    raw: RTESResponse


class RTESProtocolError(Exception):
    def __init__(self, message: str, code: str, response: RTESResponse):
        super().__init__(message)
        self.code = code
        self.response = response


class RTESClient:
    """Simple Python client for RTES line-based TCP protocol."""

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

    def produce(self, topic: str, key: str, value: str) -> ProduceResult:
        resp = self.send_command("PRODUCE", f"{topic} {key}:{value}")
        payload = self._must_payload(resp, "produce response payload missing")
        return ProduceResult(
            partition=self._extract_int_field(payload, "partition"),
            offset=self._extract_int_field(payload, "offset"),
            raw=resp,
        )

    def consume(self, topic: str, partition: int, offset: int) -> list[ConsumedMessage]:
        resp = self.send_command("CONSUME", f"{topic} {partition} {offset}")
        payload = self._must_payload(resp, "consume response payload missing")

        parts = payload.split("messages=", 1)
        data = parts[1] if len(parts) == 2 else ""
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

    def join(self, group: str, topic: str, consumer_id: str) -> JoinResult:
        resp = self.send_command("JOIN", f"{group} {topic} {consumer_id}")
        payload = self._must_payload(resp, "join response payload missing")

        start = payload.find("[")
        end = payload.find("]")
        if start == -1 or end == -1 or end <= start:
            return JoinResult(assigned=[], raw=resp)

        body = payload[start + 1 : end].strip()
        if body == "":
            return JoinResult(assigned=[], raw=resp)

        assigned = [int(v) for v in body.split()]
        return JoinResult(assigned=assigned, raw=resp)

    def commit(self, group: str, topic: str, partition: int, offset: int) -> bool:
        resp = self.send_command("COMMIT", f"{group} {topic} {partition} {offset}")
        payload = self._must_payload(resp, "commit response payload missing")
        return "committed=true" in payload

    def offset(self, group: str, topic: str, partition: int) -> int:
        resp = self.send_command("OFFSET", f"{group} {topic} {partition}")
        payload = self._must_payload(resp, "offset response payload missing")
        return self._extract_int_field(payload, "offset")

    def send_command(self, command: str, args: str = "") -> RTESResponse:
        if self._sock is None or self._file is None:
            raise RuntimeError("RTES client is not connected. Call connect() first.")

        with self._lock:
            correlation_id = str(self._correlation_seq)
            self._correlation_seq += 1

            line = f"{PROTOCOL_VERSION}|{correlation_id}|{command}|{args}\n"
            self._sock.sendall(line.encode("utf-8"))

            raw_line = self._file.readline()
            if raw_line == "":
                raise ConnectionError("RTES connection closed by server")

            response = self._parse_response(raw_line.strip())
            if response.correlation_id != correlation_id:
                raise RuntimeError(
                    f"Correlation mismatch: expected={correlation_id} got={response.correlation_id}"
                )

            if response.status == "ERR":
                raise RTESProtocolError(
                    response.message or "RTES protocol error",
                    response.code or "UNKNOWN_ERROR",
                    response,
                )

            return response

    def _parse_response(self, line: str) -> RTESResponse:
        parts = line.split("|")
        if len(parts) < 3:
            raise ValueError(f"Invalid RTES response: {line}")

        version, correlation_id, status = parts[0], parts[1], parts[2]
        if version != PROTOCOL_VERSION:
            raise ValueError(f"Unsupported RTES response version: {version}")

        if status == "OK":
            payload = "|".join(parts[3:]) if len(parts) > 3 else ""
            return RTESResponse(
                version=version,
                correlation_id=correlation_id,
                status="OK",
                payload=payload,
                raw=line,
            )

        if status == "ERR":
            code = parts[3] if len(parts) > 3 else "UNKNOWN_ERROR"
            message = "|".join(parts[4:]) if len(parts) > 4 else "unknown error"
            return RTESResponse(
                version=version,
                correlation_id=correlation_id,
                status="ERR",
                code=code,
                message=message,
                raw=line,
            )

        raise ValueError(f"Unknown RTES response status: {status}")

    @staticmethod
    def _must_payload(resp: RTESResponse, fallback: str) -> str:
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
