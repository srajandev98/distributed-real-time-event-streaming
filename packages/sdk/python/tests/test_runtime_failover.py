import socketserver
import threading
import unittest

from flux_sdk.runtime import FLUXRuntime, ProducerMessage, ProducerSendParams


class _FluxTestTCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            line = self.rfile.readline()
            if not line:
                return
            decoded = line.decode("utf-8").strip()
            if not decoded:
                continue
            parts = decoded.split("|")
            correlation_id = parts[1] if len(parts) > 1 else "0"
            command = parts[2] if len(parts) > 2 else ""
            self.server.handler_fn(self.wfile, correlation_id, command)  # type: ignore[attr-defined]


class _Server:
    def __init__(self, handler_fn):
        self._server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _FluxTestTCPHandler)
        self._server.allow_reuse_address = True
        self._server.handler_fn = handler_fn
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def port(self):
        return self._server.server_address[1]

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=1.0)


class RuntimeFailoverTests(unittest.TestCase):
    def test_producer_rotates_broker_on_not_leader(self):
        def primary_handler(wfile, correlation_id, command):
            if command == "PRODUCE":
                wfile.write(f"V1|{correlation_id}|ERR|NOT_LEADER|leader moved\n".encode("utf-8"))
            else:
                wfile.write(f"V1|{correlation_id}|OK|\n".encode("utf-8"))
            wfile.flush()

        produced_on_secondary = {"value": False}

        def secondary_handler(wfile, correlation_id, command):
            if command == "PRODUCE":
                produced_on_secondary["value"] = True
                wfile.write(f"V1|{correlation_id}|OK|partition=0 offset=0 hw=0\n".encode("utf-8"))
            else:
                wfile.write(f"V1|{correlation_id}|OK|\n".encode("utf-8"))
            wfile.flush()

        try:
            primary = _Server(primary_handler)
            secondary = _Server(secondary_handler)
        except PermissionError as err:
            self.skipTest(f"restricted sandbox socket bind: {err}")
        primary.start()
        secondary.start()
        try:
            runtime = FLUXRuntime(
                brokers=[
                    ("127.0.0.1", primary.port),
                    ("127.0.0.1", secondary.port),
                ]
            )
            producer = runtime.producer(max_retries=2, retry_backoff_ms=10)
            producer.send(
                ProducerSendParams(
                    topic="orders",
                    messages=[ProducerMessage(key="user-1", value="created")],
                    acks="1",
                )
            )
            self.assertTrue(produced_on_secondary["value"])
        finally:
            primary.stop()
            secondary.stop()


if __name__ == "__main__":
    unittest.main()
