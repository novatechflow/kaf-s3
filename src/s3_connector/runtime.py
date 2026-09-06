import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional

from .config import parse_bool
from .exceptions import DataIntegrityError
from .producer import S3Producer
from .consumer import S3Consumer

logger = logging.getLogger(__name__)

ALLOWED_METRIC_LABELS = ("topic", "partition", "reason")
COUNTABLE_METRIC_FIELDS = ("bytes",)


def escape_label_value(value: Any) -> str:
    """
    Escapes a label value for the Prometheus text exposition format.
    """
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


class MetricsRegistry:
    """
    Minimal Prometheus text-format metrics registry.
    """
    def __init__(self):
        self._counters = {}
        self._lock = threading.Lock()

    def inc(self, name: str, labels: Optional[Dict[str, Any]] = None, value: int = 1):
        key = (name, tuple(sorted((labels or {}).items())))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def render(self) -> str:
        with self._lock:
            snapshot = list(self._counters.items())

        families = {}
        for (name, labels), value in snapshot:
            families.setdefault(name, []).append((labels, value))

        lines = []
        for name in sorted(families):
            lines.append(f"# TYPE {name} counter")
            for labels, value in sorted(families[name]):
                if labels:
                    label_str = ",".join(f'{k}="{escape_label_value(v)}"' for k, v in labels)
                    lines.append(f"{name}{{{label_str}}} {value}")
                else:
                    lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


def metrics_handler(registry: MetricsRegistry):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = registry.render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # pragma: no cover - silence http.server logs
            logger.debug(format, *args)

    return Handler


def start_metrics_server(registry: MetricsRegistry, port: int, address: str = ""):
    srv = ThreadingHTTPServer((address, port), metrics_handler(registry))
    srv.daemon_threads = True
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    logger.info("Metrics server listening on %s:%s", address or "0.0.0.0", port)
    return srv


def build_config_from_env():
    kafka_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }
    if os.getenv("KAFKA_GROUP_ID"):
        kafka_config["group.id"] = os.getenv("KAFKA_GROUP_ID")
    for key in [
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "ssl.ca.location",
        "ssl.certificate.location",
        "ssl.key.location",
        "ssl.key.password",
    ]:
        env_key = f"KAFKA_{key.replace('.', '_').upper()}"
        if os.getenv(env_key):
            kafka_config[key] = os.getenv(env_key)

    if os.getenv("KAFKA_ENABLE_AUTO_COMMIT"):
        kafka_config["enable.auto.commit"] = parse_bool(os.getenv("KAFKA_ENABLE_AUTO_COMMIT"))

    dlq_topic = os.getenv("DLQ_TOPIC")
    if dlq_topic:
        kafka_config["dlq_topic"] = dlq_topic

    s3_config = {
        "bucket": os.getenv("S3_BUCKET", ""),
        "prefix": os.getenv("S3_PREFIX", ""),
        "region_name": os.getenv("AWS_REGION"),
        "endpoint_url": os.getenv("S3_ENDPOINT_URL"),
        "delete_after_consume": parse_bool(os.getenv("S3_DELETE_AFTER_CONSUME")),
        "allow_inline_payloads": parse_bool(os.getenv("S3_ALLOW_INLINE_PAYLOADS"), default=True),
        "require_integrity": parse_bool(os.getenv("S3_REQUIRE_INTEGRITY"), default=True),
        "max_inline_bytes": int(os.getenv("S3_MAX_INLINE_BYTES", "900000")),
        "max_payload_bytes": int(os.getenv("S3_MAX_PAYLOAD_BYTES", str(5 * 1024 * 1024 * 1024))),
        "deterministic_keys": parse_bool(os.getenv("S3_DETERMINISTIC_KEYS")),
        "ttl_seconds": int(os.getenv("S3_TTL_SECONDS")) if os.getenv("S3_TTL_SECONDS") else None,
        "compression": os.getenv("S3_COMPRESSION"),
        "server_side_encryption": os.getenv("S3_SSE"),
        "sse_kms_key_id": os.getenv("S3_SSE_KMS_KEY_ID"),
        "dlq_topic": dlq_topic,
    }

    config = {"kafka": kafka_config, "s3": s3_config, "hooks": {}}
    return config


def metric_hook(registry: MetricsRegistry) -> Callable[[str, Dict[str, Any]], None]:
    """
    Adapts connector events onto the registry, keeping label cardinality bounded:
    only allow-listed labels become series, and size fields become byte counters
    rather than one series per distinct size.
    """
    def hook(event: str, data: Dict[str, Any]):
        labels = {k: v for k, v in data.items() if k in ALLOWED_METRIC_LABELS and v is not None}
        registry.inc(event, labels)
        for field in COUNTABLE_METRIC_FIELDS:
            amount = data.get(field)
            if isinstance(amount, int):
                registry.inc(f"{event}_{field}", labels, amount)
    return hook


def _run_producer(config, topic, registry):
    producer = S3Producer(config)
    try:
        for line in iter(sys.stdin.buffer.readline, b""):
            payload = line.rstrip(b"\r\n")
            if not payload:
                # An empty value is a tombstone on a compacted topic, never what a
                # blank line in a pipe meant.
                registry.inc("stdin_blank_lines")
                continue
            producer.produce(topic, payload)
            registry.inc("stdin_lines")
    finally:
        producer.close()


def _run_consumer(config, topic, registry):
    consumer = S3Consumer(config)
    consumer.subscribe([topic])
    timeout = float(os.getenv("POLL_TIMEOUT", "5.0"))
    try:
        while True:
            try:
                payload = consumer.poll(timeout=timeout)
                if payload is not None and not consumer.auto_commit:
                    # Commit only once the payload is in hand, which is what makes
                    # KAFKA_ENABLE_AUTO_COMMIT=false at-least-once rather than a
                    # consumer whose offsets never advance.
                    consumer.commit(asynchronous=False)
            except DataIntegrityError as exc:
                logger.error("Integrity failure, message dropped: %s", exc)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                logger.exception("Unexpected error while consuming: %s", exc)
                registry.inc("consume_unhandled_error")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer.")
    finally:
        consumer.close()


def run():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    mode = os.getenv("MODE", "consumer")
    topic = os.getenv("TOPIC")
    if not topic:
        raise ValueError("TOPIC is required.")
    if mode not in ("producer", "consumer"):
        raise ValueError(f"Unknown MODE {mode}")

    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    metrics_address = os.getenv("METRICS_ADDRESS", "")
    registry = MetricsRegistry()
    start_metrics_server(registry, metrics_port, metrics_address)

    config = build_config_from_env()
    config["hooks"]["metrics"] = metric_hook(registry)

    if mode == "producer":
        _run_producer(config, topic, registry)
    else:
        _run_consumer(config, topic, registry)


if __name__ == "__main__":
    run()
