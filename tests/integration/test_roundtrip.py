"""
Producer to consumer round trips against a real broker and a real S3 service.
"""
import hashlib
import os

import pytest

from s3_connector import DataIntegrityError, S3Consumer, S3Producer

from .helpers import BUCKET, consume_one

pytestmark = pytest.mark.integration

SMALL = b"order-event-" * 64
LARGE = os.urandom(64 * 1024)


@pytest.mark.parametrize("label,s3_options,payload", [
    ("plain", {"max_inline_bytes": 0}, SMALL),
    ("gzip", {"max_inline_bytes": 0, "compression": "gzip"}, SMALL),
    ("prefix", {"max_inline_bytes": 0, "prefix": "kafka/events"}, SMALL),
    ("deterministic", {"max_inline_bytes": 0, "deterministic_keys": True}, SMALL),
    ("sse", {"max_inline_bytes": 0, "server_side_encryption": "AES256"}, SMALL),
    ("ttl", {"max_inline_bytes": 0, "ttl_seconds": 86400}, SMALL),
    ("incompressible", {"max_inline_bytes": 0, "compression": "gzip"}, LARGE),
    ("inline", {"max_inline_bytes": 1_000_000}, SMALL),
])
def test_round_trip(producer_config, consumer_config, topic, label, s3_options, payload):
    producer = S3Producer(producer_config(**s3_options))
    producer.produce(topic, payload)
    assert producer.close() == 0

    consumer = S3Consumer(consumer_config(**s3_options))
    consumer.subscribe([topic])
    try:
        assert consume_one(consumer) == payload
    finally:
        consumer.close()


def test_multipart_round_trip(producer_config, consumer_config, topic):
    """
    Above the threshold boto3 uploads in parts. A multipart ETag is a hash of
    part hashes, not of the content, which is why the reference omits it.
    """
    payload = os.urandom(12 * 1024 * 1024)
    options = {"max_inline_bytes": 0, "multipart_threshold": 5 * 1024 * 1024}

    producer = S3Producer(producer_config(**options))
    producer.produce(topic, payload)
    assert producer.close() == 0

    consumer = S3Consumer(consumer_config(**options))
    consumer.subscribe([topic])
    try:
        assert consume_one(consumer, timeout=120) == payload
    finally:
        consumer.close()


def test_inline_json_object_is_delivered_unchanged(producer_config, consumer_config, topic):
    """Regression guard: these were parsed as references and silently dropped."""
    payload = b'{"order_id": 42, "status": "paid", "s3_note": "not a reference"}'

    producer = S3Producer(producer_config(max_inline_bytes=1_000_000))
    producer.produce(topic, payload)
    assert producer.close() == 0

    consumer = S3Consumer(consumer_config())
    consumer.subscribe([topic])
    try:
        assert consume_one(consumer) == payload
    finally:
        consumer.close()


def test_keys_land_under_the_configured_prefix(producer_config, s3, topic):
    producer = S3Producer(producer_config(max_inline_bytes=0, prefix="kafka/events"))
    producer.produce(topic, SMALL)
    assert producer.close() == 0

    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="kafka/events/")
    assert listing["KeyCount"] >= 1


def test_reference_outside_the_prefix_is_rejected(producer_config, consumer_config, topic):
    producer = S3Producer(producer_config(max_inline_bytes=0, prefix="somewhere/else"))
    producer.produce(topic, SMALL)
    assert producer.close() == 0

    consumer = S3Consumer(consumer_config(prefix="kafka/events"))
    consumer.subscribe([topic])
    try:
        with pytest.raises(DataIntegrityError, match="outside allowed prefix"):
            consume_one(consumer)
    finally:
        consumer.close()


def test_tampered_object_fails_the_checksum(producer_config, consumer_config, s3, topic):
    """The integrity check has to catch a change made behind the connector's back."""
    producer = S3Producer(producer_config(max_inline_bytes=0))
    producer.produce(topic, SMALL)
    assert producer.close() == 0

    key = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(key, key=lambda o: o["LastModified"])["Key"]
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"tampered")

    consumer = S3Consumer(consumer_config())
    consumer.subscribe([topic])
    try:
        with pytest.raises(DataIntegrityError):
            consume_one(consumer)
    finally:
        consumer.close()


def test_missing_object_is_skipped_not_raised(producer_config, consumer_config, s3, topic):
    producer = S3Producer(producer_config(max_inline_bytes=0))
    producer.produce(topic, SMALL)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    s3.delete_object(Bucket=BUCKET, Key=key)

    skips = []
    config = consumer_config()
    config["hooks"] = {"skipped": lambda reason, data: skips.append(reason)}
    consumer = S3Consumer(config)
    consumer.subscribe([topic])
    try:
        assert consume_one(consumer, timeout=20) is None
        assert "object_missing" in skips
    finally:
        consumer.close()


def test_sha256_matches_the_payload_not_the_stored_bytes(producer_config, s3, topic):
    """With compression the stored bytes differ from the payload; sha256 covers the payload."""
    payload = b"compress me " * 500
    producer = S3Producer(producer_config(max_inline_bytes=0, compression="gzip"))
    producer.produce(topic, payload)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    stored = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    assert stored != payload
    assert hashlib.sha256(stored).hexdigest() != hashlib.sha256(payload).hexdigest()
