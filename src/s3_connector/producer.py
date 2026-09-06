import boto3
import gzip
import hashlib
import json
import logging
import time
import uuid
from confluent_kafka import Producer

from .config import build_s3_client, split_kafka_config

logger = logging.getLogger(__name__)

class S3Producer:
    def __init__(self, config):
        """
        Initializes the S3Producer.

        :param config: A dictionary with 'kafka' and 's3' configurations.
        """
        kafka_config = config.get("kafka", {})
        self.s3_config = config.get("s3", {})
        self.hooks = config.get("hooks", {})

        if "bucket" not in self.s3_config:
            raise ValueError("S3 bucket must be specified in the configuration.")

        client_config, kafka_dlq_topic = split_kafka_config(kafka_config)
        self.kafka_producer = Producer(client_config)
        self.dlq_topic = kafka_dlq_topic or self.s3_config.get("dlq_topic")
        self.s3_client = build_s3_client(boto3, self.s3_config)
        self.s3_bucket = self.s3_config["bucket"]
        self.max_inline_bytes = self.s3_config.get("max_inline_bytes", 900_000)
        self.max_payload_bytes = self.s3_config.get("max_payload_bytes", 5 * 1024 * 1024 * 1024)
        self.s3_prefix = self.s3_config.get("prefix", "").rstrip("/")
        self.deterministic_keys = self.s3_config.get("deterministic_keys", False)
        self.ttl_seconds = self.s3_config.get("ttl_seconds")
        self.compression = self.s3_config.get("compression")
        self.sse = self.s3_config.get("server_side_encryption")
        self.sse_kms_key_id = self.s3_config.get("sse_kms_key_id")
        self.metric_callback = self.hooks.get("metrics")

        if self.max_inline_bytes < 0:
            raise ValueError("max_inline_bytes must be non-negative.")
        if self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive.")
        if self.max_inline_bytes > self.max_payload_bytes:
            raise ValueError("max_inline_bytes cannot exceed max_payload_bytes.")
        if self.compression not in (None, "gzip"):
            raise ValueError("Unsupported compression algorithm.")

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
            self.kafka_producer.produce(topic, key=key, value=bytes(payload))
            self.kafka_producer.poll(0)
            self._emit_metric("produce_inline", topic=topic, bytes=payload_length)
            return

        s3_key = self._build_s3_key(payload)
        payload_to_store = payload
        compression = None
        if self.compression == "gzip":
            payload_to_store = gzip.compress(payload)
            compression = "gzip"

        put_kwargs = dict(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=payload_to_store
        )
        if self.ttl_seconds:
            put_kwargs["Metadata"] = {"ttl_epoch": str(int(time.time()) + int(self.ttl_seconds))}
        if self.sse:
            put_kwargs["ServerSideEncryption"] = self.sse
        if self.sse_kms_key_id:
            put_kwargs["SSEKMSKeyId"] = self.sse_kms_key_id

        response = self.s3_client.put_object(**put_kwargs)

        etag = response.get("ETag", "").strip('"')
        sha256 = hashlib.sha256(payload).hexdigest()

        reference_message = {
            "s3_bucket": self.s3_bucket,
            "s3_key": s3_key,
            "etag": etag,
            "sha256": sha256,
            "compression": compression,
        }

        try:
            self.kafka_producer.produce(
                topic,
                key=key,
                value=json.dumps(reference_message).encode('utf-8'),
                on_delivery=lambda err, msg: self._on_delivery(err, msg, s3_key, reference_message),
            )
        except Exception:
            self._cleanup_s3_object(s3_key)
            raise

        self.kafka_producer.poll(0)
        self._emit_metric("produce_offloaded", topic=topic, bytes=payload_length, s3_key=s3_key)

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
        Delivery callback to clean up S3 objects when Kafka delivery fails.
        """
        if err:
            logger.error("Kafka delivery failed for S3 key %s: %s", s3_key, err)
            self._cleanup_s3_object(s3_key)
            self._emit_metric("produce_error", error=str(err), s3_key=s3_key)
            if self.dlq_topic:
                try:
                    dlq_payload = json.dumps({"error": str(err), "reference": reference_message}).encode("utf-8")
                    self.kafka_producer.produce(self.dlq_topic, value=dlq_payload)
                except Exception as dlq_err:  # pragma: no cover - best effort
                    logger.warning("Failed to publish to DLQ %s: %s", self.dlq_topic, dlq_err)
        else:
            self._emit_metric("produce_delivered", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())

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
