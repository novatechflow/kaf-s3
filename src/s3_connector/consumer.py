import boto3
import gzip
import hashlib
import io
import json
import logging
from confluent_kafka import Consumer, KafkaError, Producer

from .config import build_s3_client, split_kafka_config
from .exceptions import DataIntegrityError

logger = logging.getLogger(__name__)

class S3Consumer:
    def __init__(self, config):
        """
        Initializes the S3Consumer.

        :param config: A dictionary with 'kafka' and 's3' configurations.
        """
        kafka_config = config.get("kafka", {})
        self.s3_config = config.get("s3", {})
        self.hooks = config.get("hooks", {})

        if "group.id" not in kafka_config:
            raise ValueError("Kafka consumer 'group.id' must be specified.")
        if "bucket" not in self.s3_config:
            raise ValueError("S3 bucket must be specified in the configuration.")

        client_config, kafka_dlq_topic = split_kafka_config(kafka_config)
        self.kafka_consumer = Consumer(client_config)
        self.s3_client = build_s3_client(boto3, self.s3_config)
        self.s3_bucket = self.s3_config["bucket"]
        self.delete_after_consume = self.s3_config.get("delete_after_consume", False)
        self.allow_inline_payloads = self.s3_config.get("allow_inline_payloads", True)
        self.expected_prefix = self.s3_config.get("prefix", "").rstrip("/")
        self.max_payload_bytes = self.s3_config.get("max_payload_bytes", 5 * 1024 * 1024 * 1024)
        self.require_integrity = self.s3_config.get("require_integrity", True)
        self.metric_callback = self.hooks.get("metrics")
        self.skip_callback = self.hooks.get("skipped")
        self.dlq_topic = kafka_dlq_topic or self.s3_config.get("dlq_topic")
        self.dlq_producer = Producer(client_config) if self.dlq_topic else None

        if self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive.")

    def close(self):
        """
        Flushes any pending DLQ records and closes the Kafka consumer.
        """
        if self.dlq_producer:
            remaining = self.dlq_producer.flush(30.0)
            if remaining:
                logger.error("%s DLQ record(s) still undelivered after flush timeout.", remaining)
        self.kafka_consumer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def subscribe(self, topics):
        """
        Subscribes the consumer to a list of topics.
        """
        self.kafka_consumer.subscribe(topics)

    def commit(self, message=None, asynchronous=True):
        """
        Commits offsets. Use with 'enable.auto.commit': False for at-least-once
        delivery, committing only after the payload has been processed.
        """
        if message is None:
            return self.kafka_consumer.commit(asynchronous=asynchronous)
        return self.kafka_consumer.commit(message=message, asynchronous=asynchronous)

    def poll(self, timeout=1.0):
        """
        Polls for a message, downloads the payload from S3, and returns it.

        Returns None both when no message is available and when a message was
        skipped; subscribe to the 'skipped' hook to distinguish the two.

        :param timeout: The maximum time to block waiting for a message.
        :return: The downloaded payload (bytes) or None.
        """
        msg = self.kafka_consumer.poll(timeout)

        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            else:
                raise Exception(msg.error())

        raw_value = msg.value()

        if raw_value is None:
            return self._skip("tombstone", topic=msg.topic())

        ref_message = self._parse_reference(raw_value)
        if ref_message is None:
            if self.allow_inline_payloads:
                self._emit_metric("consume_inline", topic=msg.topic(), bytes=len(raw_value))
                return raw_value
            return self._skip("malformed_message", topic=msg.topic(), raw=raw_value)

        s3_bucket = ref_message.get("s3_bucket")
        s3_key = ref_message.get("s3_key")
        if not isinstance(s3_bucket, str) or not isinstance(s3_key, str):
            return self._skip("missing_key", topic=msg.topic(), reference=ref_message)

        if s3_bucket != self.s3_bucket:
            raise self._integrity_error(
                f"Unexpected S3 bucket in message: {s3_bucket}", ref_message
            )
        if self.expected_prefix and not s3_key.startswith(self.expected_prefix + "/"):
            raise self._integrity_error(f"S3 key outside allowed prefix: {s3_key}", ref_message)

        expected_etag = ref_message.get("etag")
        expected_sha = ref_message.get("sha256")
        compression = ref_message.get("compression")

        if self.require_integrity and not (expected_etag or expected_sha):
            raise self._integrity_error(
                f"Reference for {s3_key} carries no etag or sha256 to verify against", ref_message
            )
        if compression not in (None, "gzip"):
            raise self._integrity_error(
                f"Unsupported compression '{compression}' for S3 object {s3_key}", ref_message
            )

        response = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        stored = self._read_capped(response["Body"], s3_key, ref_message)

        actual_etag = response.get("ETag", "").strip('"')
        if expected_etag:
            if not actual_etag:
                if self.require_integrity:
                    raise self._integrity_error(
                        f"S3 object {s3_key} returned no ETag to verify", ref_message
                    )
            elif actual_etag != expected_etag:
                raise self._integrity_error(f"ETag check failed for S3 object {s3_key}", ref_message)

        body = self._decompress(stored, s3_key, ref_message) if compression == "gzip" else stored

        if expected_sha:
            actual_sha = hashlib.sha256(body).hexdigest()
            if actual_sha != expected_sha:
                raise self._integrity_error(f"SHA-256 check failed for S3 object {s3_key}", ref_message)

        if self.delete_after_consume:
            try:
                self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                logger.warning("Failed to delete S3 object %s after consume: %s", s3_key, exc)

        self._emit_metric("consume_success", topic=msg.topic(), bytes=len(body), s3_key=s3_key)
        return body

    def _parse_reference(self, raw_value):
        """
        Decodes a reference message, or None if the payload is not one.
        """
        try:
            decoded = json.loads(raw_value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.debug("Payload is not a JSON reference message: %s", exc)
            return None
        if not isinstance(decoded, dict):
            logger.debug("Payload is JSON but not a reference object: %s", type(decoded).__name__)
            return None
        return decoded

    def _read_capped(self, body_stream, s3_key, ref_message):
        """
        Reads an S3 body, refusing objects larger than max_payload_bytes.
        """
        limit = self.max_payload_bytes
        data = body_stream.read(limit + 1)
        if len(data) > limit:
            raise self._integrity_error(
                f"S3 object {s3_key} exceeds max_payload_bytes {limit}", ref_message
            )
        return data

    def _decompress(self, stored, s3_key, ref_message):
        """
        Decompresses gzip payloads, bounded by max_payload_bytes so that a small
        object cannot expand into an unbounded allocation.
        """
        limit = self.max_payload_bytes
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(stored)) as gz:
                body = gz.read(limit + 1)
        except (OSError, EOFError) as exc:
            raise self._integrity_error(
                f"Failed to decompress S3 object {s3_key}: {exc}", ref_message
            )
        if len(body) > limit:
            raise self._integrity_error(
                f"Decompressed S3 object {s3_key} exceeds max_payload_bytes {limit}", ref_message
            )
        return body

    def _integrity_error(self, message, reference):
        """
        Builds a DataIntegrityError and mirrors it to the DLQ.
        """
        err = DataIntegrityError(message)
        self._send_dlq(error=str(err), reference=reference)
        self._emit_metric("consume_integrity_error", reason=message)
        return err

    def _skip(self, reason, topic=None, reference=None, raw=None):
        """
        Records a skipped message and returns None to the caller.
        """
        logger.warning("Skipping message (%s)", reason)
        self._send_dlq(error=reason, reference=reference, raw=raw)
        self._emit_metric("consume_skipped", topic=topic, reason=reason)
        if self.skip_callback:
            try:
                self.skip_callback(reason, {"topic": topic, "reference": reference})
            except Exception:  # pragma: no cover - user callback errors ignored
                logger.debug("Skip callback failed for %s", reason)
        return None

    def _send_dlq(self, error, reference=None, raw=None):
        """
        Best-effort DLQ publication.
        """
        if not self.dlq_topic or not self.dlq_producer:
            return
        payload = {"error": error}
        if reference is not None:
            payload["reference"] = reference
        if raw is not None:
            payload["raw"] = raw.decode("utf-8", errors="replace")
        try:
            self.dlq_producer.produce(self.dlq_topic, value=json.dumps(payload).encode("utf-8"))
            self.dlq_producer.poll(0)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to publish to DLQ %s: %s", self.dlq_topic, exc)

    def _emit_metric(self, event, **data):
        """
        Send metric events to optional callback.
        """
        if self.metric_callback:
            try:
                self.metric_callback(event, data)
            except Exception:  # pragma: no cover
                logger.debug("Metric callback failed for %s", event)
