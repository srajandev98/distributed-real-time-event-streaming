from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from .client import ConsumedMessage, FLUXClient, FLUXProtocolError


def _sleep_ms(ms: int) -> None:
    time.sleep(ms / 1000.0)


def _is_generation_mismatch(err: Exception) -> bool:
    return isinstance(err, FLUXProtocolError) and err.code == "GENERATION_MISMATCH"


def _is_retryable_broker_error(err: Exception) -> bool:
    if isinstance(err, FLUXProtocolError) and err.code == "NOT_LEADER":
        return True
    message = str(err)
    return (
        "connection closed" in message
        or "Connection refused" in message
        or "timed out" in message
        or "socket hang up" in message
    )


def _with_retry(
    operation_name: str,
    fn: Callable[[], object],
    max_retries: int,
    retry_backoff_ms: int,
) -> object:
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as err:
            attempt += 1
            if attempt > max_retries:
                raise
            backoff = retry_backoff_ms * attempt
            print(
                f"{operation_name} failed (attempt {attempt}/{max_retries + 1}): {err}"
            )
            _sleep_ms(backoff)


@dataclass
class ProducerMessage:
    key: str
    value: str


@dataclass
class ProducerSendParams:
    topic: str
    messages: List[ProducerMessage]
    acks: str = "1"


@dataclass
class ConsumerRunContext:
    topic: str
    partition: int
    message: ConsumedMessage


class FLUXProducer:
    def __init__(
        self,
        client: FLUXClient,
        max_retries: int = 3,
        retry_backoff_ms: int = 250,
        batch_size: int = 100,
        linger_ms: int = 0,
    ):
        self._client = client
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._batch_size = batch_size
        self._linger_ms = linger_ms

    def send(self, params: ProducerSendParams) -> None:
        if len(params.messages) == 0:
            return

        _with_retry(
            "producer.connect",
            self._client.connect,
            self._max_retries,
            self._retry_backoff_ms,
        )
        try:
            for i in range(0, len(params.messages), self._batch_size):
                batch = params.messages[i : i + self._batch_size]
                for msg in batch:
                    _with_retry(
                        "producer.produce",
                        lambda m=msg: self._client.produce(
                            params.topic, m.key, m.value, params.acks
                        ),
                        self._max_retries,
                        self._retry_backoff_ms,
                    )
                if self._linger_ms > 0 and i + self._batch_size < len(params.messages):
                    _sleep_ms(self._linger_ms)
        finally:
            self._client.close()


class FLUXConsumer:
    def __init__(
        self,
        client: FLUXClient,
        group_id: str,
        consumer_id: str,
        assignor: str = "round_robin",
        heartbeat_interval_ms: int = 2000,
        poll_interval_ms: int = 500,
        auto_commit: bool = True,
        auto_rejoin: bool = True,
        max_retries: int = 3,
        retry_backoff_ms: int = 250,
        on_assign: Optional[Callable[[List[int]], None]] = None,
        on_revoke: Optional[Callable[[List[int]], None]] = None,
        on_crash: Optional[Callable[[Exception], None]] = None,
    ):
        self._client = client
        self._group_id = group_id
        self._consumer_id = consumer_id
        self._assignor = assignor
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._poll_interval_ms = poll_interval_ms
        self._auto_commit = auto_commit
        self._auto_rejoin = auto_rejoin
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._on_assign = on_assign
        self._on_revoke = on_revoke
        self._on_crash = on_crash

        self._topic = ""
        self._generation = 0
        self._assigned: List[int] = []
        self._running = False
        self._rejoin_needed = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop_event = threading.Event()

    def subscribe(self, topic: str) -> None:
        self._topic = topic

    def _run_assign_callback(self, next_assigned: List[int]) -> None:
        prev = sorted(self._assigned)
        next_sorted = sorted(next_assigned)
        changed = len(prev) != len(next_sorted) or any(
            value != next_sorted[idx] for idx, value in enumerate(prev)
        )
        if not changed:
            self._assigned = list(next_assigned)
            return

        if len(self._assigned) > 0 and self._on_revoke is not None:
            self._on_revoke(list(self._assigned))
        self._assigned = list(next_assigned)
        if self._on_assign is not None:
            self._on_assign(list(self._assigned))

    def _join_and_sync(self) -> None:
        joined = _with_retry(
            "consumer.join",
            lambda: self._client.join(
                self._group_id, self._topic, self._consumer_id, self._assignor
            ),
            self._max_retries,
            self._retry_backoff_ms,
        )
        self._generation = joined.generation

        synced = _with_retry(
            "consumer.sync",
            lambda: self._client.sync(
                self._group_id, self._topic, self._consumer_id, self._generation
            ),
            self._max_retries,
            self._retry_backoff_ms,
        )
        self._generation = synced.generation
        self._run_assign_callback(synced.assigned)
        self._rejoin_needed = False

    def _start_heartbeat(self) -> None:
        self._heartbeat_stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1.0)
        self._heartbeat_stop_event.clear()

        def loop() -> None:
            while not self._heartbeat_stop_event.is_set():
                try:
                    self._client.heartbeat(
                        self._group_id,
                        self._topic,
                        self._consumer_id,
                        self._generation,
                    )
                except Exception as err:
                    if _is_generation_mismatch(err):
                        self._rejoin_needed = True
                    elif self._running:
                        self._rejoin_needed = True
                self._heartbeat_stop_event.wait(self._heartbeat_interval_ms / 1000.0)

        self._heartbeat_thread = threading.Thread(target=loop, daemon=True)
        self._heartbeat_thread.start()

    def run(self, each_message: Callable[[ConsumerRunContext], None]) -> None:
        if self._topic == "":
            raise ValueError("consumer.subscribe(topic) must be called before run()")
        if self._running:
            raise RuntimeError("consumer is already running")

        self._running = True
        _with_retry(
            "consumer.connect",
            self._client.connect,
            self._max_retries,
            self._retry_backoff_ms,
        )
        self._join_and_sync()
        self._start_heartbeat()

        try:
            while self._running:
                if self._rejoin_needed:
                    if not self._auto_rejoin:
                        raise RuntimeError(
                            "consumer requires rejoin but auto_rejoin=False"
                        )
                    self._join_and_sync()
                    self._start_heartbeat()

                for partition in list(self._assigned):
                    if not self._running:
                        break
                    try:
                        start_offset = _with_retry(
                            "consumer.offset",
                            lambda p=partition: self._client.offset(
                                self._group_id, self._topic, p
                            ),
                            self._max_retries,
                            self._retry_backoff_ms,
                        )
                        messages = _with_retry(
                            "consumer.consume",
                            lambda p=partition, o=start_offset: self._client.consume(
                                self._topic, p, o
                            ),
                            self._max_retries,
                            self._retry_backoff_ms,
                        )

                        for message in messages:
                            each_message(
                                ConsumerRunContext(
                                    topic=self._topic,
                                    partition=partition,
                                    message=message,
                                )
                            )

                        if self._auto_commit and len(messages) > 0:
                            next_offset = messages[-1].offset + 1
                            _with_retry(
                                "consumer.commit",
                                lambda p=partition, o=next_offset: self._client.commit(
                                    self._group_id,
                                    self._topic,
                                    self._consumer_id,
                                    self._generation,
                                    p,
                                    o,
                                ),
                                self._max_retries,
                                self._retry_backoff_ms,
                            )
                    except Exception as err:
                        if _is_generation_mismatch(err) and self._auto_rejoin:
                            self._rejoin_needed = True
                            break
                        raise
                _sleep_ms(self._poll_interval_ms)
        except Exception as err:
            if self._on_crash is not None:
                self._on_crash(err)
            raise
        finally:
            self._heartbeat_stop_event.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=1.0)
                self._heartbeat_thread = None
            if len(self._assigned) > 0 and self._on_revoke is not None:
                self._on_revoke(list(self._assigned))

    def disconnect(self) -> None:
        self._running = False
        self._heartbeat_stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1.0)
            self._heartbeat_thread = None
        if self._generation > 0 and self._topic:
            try:
                self._client.leave(
                    self._group_id, self._topic, self._consumer_id, self._generation
                )
            except Exception:
                pass
        self._client.close()


class FLUXRuntime:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9092,
        timeout_seconds: float = 5.0,
        brokers: Optional[List[tuple[str, int]]] = None,
    ):
        self._brokers = brokers if brokers is not None and len(brokers) > 0 else [(host, port)]
        self._timeout_seconds = timeout_seconds
        self._endpoint_index = 0

    def _next_client(self) -> FLUXClient:
        host, port = self._brokers[self._endpoint_index]
        return FLUXClient(host=host, port=port, timeout_seconds=self._timeout_seconds)

    def _rotate_client(self) -> FLUXClient:
        self._endpoint_index = (self._endpoint_index + 1) % len(self._brokers)
        return self._next_client()

    def producer(
        self,
        max_retries: int = 3,
        retry_backoff_ms: int = 250,
        batch_size: int = 100,
        linger_ms: int = 0,
    ) -> FLUXProducer:
        client = self._next_client()

        def wrapped_send(params: ProducerSendParams) -> None:
            nonlocal client
            attempts = 0
            max_endpoint_attempts = max(1, len(self._brokers))
            while attempts < max_endpoint_attempts:
                producer = FLUXProducer(
                    client=client,
                    max_retries=max_retries,
                    retry_backoff_ms=retry_backoff_ms,
                    batch_size=batch_size,
                    linger_ms=linger_ms,
                )
                try:
                    producer.send(params)
                    return
                except Exception as err:
                    attempts += 1
                    if not _is_retryable_broker_error(err) or attempts >= max_endpoint_attempts:
                        raise
                    client = self._rotate_client()

        class _ProducerWrapper:
            def send(self, params: ProducerSendParams) -> None:
                wrapped_send(params)

        return _ProducerWrapper()  # type: ignore[return-value]

    def consumer(
        self,
        group_id: str,
        consumer_id: str,
        assignor: str = "round_robin",
        heartbeat_interval_ms: int = 2000,
        poll_interval_ms: int = 500,
        auto_commit: bool = True,
        auto_rejoin: bool = True,
        max_retries: int = 3,
        retry_backoff_ms: int = 250,
        on_assign: Optional[Callable[[List[int]], None]] = None,
        on_revoke: Optional[Callable[[List[int]], None]] = None,
        on_crash: Optional[Callable[[Exception], None]] = None,
    ) -> FLUXConsumer:
        return FLUXConsumer(
            client=self._next_client(),
            group_id=group_id,
            consumer_id=consumer_id,
            assignor=assignor,
            heartbeat_interval_ms=heartbeat_interval_ms,
            poll_interval_ms=poll_interval_ms,
            auto_commit=auto_commit,
            auto_rejoin=auto_rejoin,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_crash=on_crash,
        )
