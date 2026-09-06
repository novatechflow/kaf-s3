import boto3
import gzip
import hashlib
import io
import json
import logging
import time
import uuid
from confluent_kafka import Producer

from .config import build_s3_client, require_bucket, split_kafka_config

logger = logging.getLogger(__name__)

# S3 accepts at most 5 GiB in a single PUT; larger objects require multipart.
SINGLE_PUT_LIMIT_BYTES = 5 * 1024 * 1024 * 1024
DEFAULT_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024
DEFAULT_PRODUCE_TIMEOUT_SECONDS = 30.0

class S3Producer:
    def __init__(self, config):
        """
        Initializes the S3Producer.

        :param config: A dictionary with 'kafka' and 's3' configurations.
        """
        kafka_config = config.get("kafka", {})
        self.s3_config = config.get("s3", {})
        self.hooks = config.get("hooks", {})

        self.s3_bucket = require_bucket(self.s3_config)

        client_config, kafka_dlq_topic = split_kafka_config(kafka_config)
        self.kafka_producer = Producer(client_config)
        self.dlq_topic = kafka_dlq_topic or self.s3_config.get("dlq_topic")
        self.s3_client = build_s3_client(boto3, self.s3_config)
        self.max_inline_bytes = self.s3_config.get("max_inline_bytes", 900_000)
        self.max_payload_bytes = self.s3_config.get("max_payload_bytes", 5 * 1024 * 1024 * 1024)
        self.s3_prefix = self.s3_config.get("prefix", "").rstrip("/")
        if self.s3_config.get("prefix") and not self.s3_prefix:
            raise ValueError("S3 prefix must contain more than '/'.")
        self.deterministic_keys = self.s3_config.get("deterministic_keys", False)
        self.ttl_seconds = self.s3_config.get("ttl_seconds")
        self.compression = self.s3_config.get("compression")
        self.sse = self.s3_config.get("server_side_encryption")
        self.sse_kms_key_id = self.s3_config.get("sse_kms_key_id")
        self.multipart_threshold = self.s3_config.get(
            "multipart_threshold", DEFAULT_MULTIPART_THRESHOLD_BYTES
        )
        self.produce_timeout = self.s3_config.get(
            "produce_timeout", DEFAULT_PRODUCE_TIMEOUT_SECONDS
        )
        self.metric_callback = self.hooks.get("metrics")

        if self.max_inline_bytes < 0:
            raise ValueError("max_inline_bytes must be non-negative.")
        if self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive.")
        if self.max_inline_bytes > self.max_payload_bytes:
            raise ValueError("max_inline_bytes cannot exceed max_payload_bytes.")
        if self.compression not in (None, "gzip"):
            raise ValueError("Unsupported compression algorithm.")
        if self.sse not in (None, "AES256", "aws:kms"):
            raise ValueError("server_side_encryption must be 'AES256' or 'aws:kms'.")
        if self.sse_kms_key_id and self.sse != "aws:kms":
            raise ValueError("sse_kms_key_id requires server_side_encryption='aws:kms'.")
        if not 0 < self.multipart_threshold <= SINGLE_PUT_LIMIT_BYTES:
            raise ValueError(
                f"multipart_threshold must be between 1 and {SINGLE_PUT_LIMIT_BYTES}."
            )
        if self.produce_timeout < 0:
            raise ValueError("produce_timeout must be non-negative.")

    def produce(self, topic, payload, key=None):
        """
        Uploads a large payload to S3 and sends a reference message to Kafka.

        :param topic: The Kafka topic to produce to.
        :param payload: The large message payload (bytes).
        :param key: The Kafka message key.
        """
        if not isinstance(payload, (bytes, bytearray)):
            raise ValueError("Payload must be bytes or bytearray.")

        payload_length = len(payload)

        if payload_length > self.max_payload_bytes:
            raise ValueError(f"Payload size {payload_length} exceeds max_payload_bytes {self.max_payload_bytes}.")

        if payload_length <= self.max_inline_bytes:
            self._produce(
                topic,
                key,
                bytes(payload),
                on_delivery=lambda err, msg: self._on_delivery(err, msg, None, None),
            )
            self.kafka_producer.poll(0)
            self._emit_metric("produce_inline", topic=topic, bytes=payload_length)
            return

        s3_key = self._build_s3_key(payload)
        payload_to_store = payload
        compression = None
        if self.compression == "gzip":
            payload_to_store = gzip.compress(payload)
            compression = "gzip"

        etag = self._upload(s3_key, payload_to_store)
        sha256 = hashlib.sha256(payload).hexdigest()

        reference_message = {
            "s3_bucket": self.s3_bucket,
            "s3_key": s3_key,
            "etag": etag,
            "sha256": sha256,
            "compression": compression,
        }

        try:
            self._produce(
                topic,
                key,
                json.dumps(reference_message).encode('utf-8'),
                on_delivery=lambda err, msg: self._on_delivery(err, msg, s3_key, reference_message),
            )
        except Exception:
            self._cleanup_s3_object(s3_key)
            raise

        self.kafka_producer.poll(0)
        self._emit_metric("produce_offloaded", topic=topic, bytes=payload_length, s3_key=s3_key)

    def _produce(self, topic, key, value, on_delivery=None):
        """
        Enqueues a message, applying backpressure instead of failing outright
        when librdkafka's local queue is full.

        produce() raises BufferError once the queue reaches
        queue.buffering.max.messages. Serving delivery reports drains it, so a
        burst waits rather than losing the message it has already uploaded.
        """
        deadline = time.monotonic() + self.produce_timeout
        while True:
            try:
                self.kafka_producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                if time.monotonic() >= deadline:
                    self._emit_metric("produce_queue_full", topic=topic)
                    raise
                self.kafka_producer.poll(0.1)

    def _object_kwargs(self):
        """
        Encryption and metadata options shared by both upload paths.
        """
        kwargs = {}
        if self.ttl_seconds:
            kwargs["Metadata"] = {"ttl_epoch": str(int(time.time()) + int(self.ttl_seconds))}
        if self.sse:
            kwargs["ServerSideEncryption"] = self.sse
        if self.sse_kms_key_id:
            kwargs["SSEKMSKeyId"] = self.sse_kms_key_id
        return kwargs

    def _upload(self, s3_key, body):
        """
        Uploads the stored bytes, switching to multipart above the threshold so
        that objects larger than S3's single-PUT limit still succeed.

        :return: The object ETag, or None for multipart uploads where the ETag
                 is not a checksum of the content. SHA-256 covers those.
        """
        if len(body) <= self.multipart_threshold:
            response = self.s3_client.put_object(
                Bucket=self.s3_bucket, Key=s3_key, Body=body, **self._object_kwargs()
            )
            return response.get("ETag", "").strip('"')

        self.s3_client.upload_fileobj(
            io.BytesIO(body),
            self.s3_bucket,
            s3_key,
            ExtraArgs=self._object_kwargs() or None,
        )
        return None

    def flush(self, timeout=30.0):
        """
        Blocks until queued messages are delivered or the timeout expires.

        :return: The number of messages still undelivered.
        """
        return self.kafka_producer.flush(timeout)

    def close(self, timeout=30.0):
        """
        Flushes outstanding messages and logs any that could not be delivered.
        """
        remaining = self.flush(timeout)
        if remaining:
            logger.error("%s message(s) still undelivered after flush timeout.", remaining)
        return remaining

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _cleanup_s3_object(self, s3_key):
        """
        Best-effort cleanup of an S3 object, used when Kafka publish fails.

        Skipped for deterministic keys, where the same object may still be
        referenced by another successfully delivered message.
        """
        if self.deterministic_keys:
            logger.warning(
                "Leaving S3 object %s in place after produce error: deterministic keys may be shared.",
                s3_key,
            )
            return
        try:
            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
        except Exception as exc:  # pragma: no cover - best-effort cleanup
            logger.warning("Failed to clean up S3 object %s after produce error: %s", s3_key, exc)

    def _on_delivery(self, err, msg, s3_key, reference_message):
        """
        Delivery callback for both paths. Inline messages carry no S3 key, so
        they are reported but have nothing to roll back.
        """
        if not err:
            self._emit_metric(
                "produce_delivered", topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            return

        if s3_key is None:
            logger.error("Kafka delivery failed for inline message: %s", err)
        else:
            logger.error("Kafka delivery failed for S3 key %s: %s", s3_key, err)
            self._cleanup_s3_object(s3_key)
        self._emit_metric("produce_error", error=str(err), s3_key=s3_key)

        if self.dlq_topic:
            try:
                dlq_payload = json.dumps(
                    {"error": str(err), "reference": reference_message}
                ).encode("utf-8")
                self.kafka_producer.produce(self.dlq_topic, value=dlq_payload)
            except Exception as dlq_err:  # pragma: no cover - best effort
                logger.warning("Failed to publish to DLQ %s: %s", self.dlq_topic, dlq_err)

    def _build_s3_key(self, payload):
        """
        Builds an S3 key, optionally deterministic and prefixed.
        """
        if self.deterministic_keys:
            base = hashlib.sha256(payload).hexdigest()
        else:
            base = str(uuid.uuid4())

        if self.s3_prefix:
            return f"{self.s3_prefix}/{base}"
        return base

    def _emit_metric(self, event, **data):
        """
        Send metric events to optional callback.
        """
        if self.metric_callback:
            try:
                self.metric_callback(event, data)
            except Exception:  # pragma: no cover - user callback errors ignored
                logger.debug("Metric callback failed for %s", event)
