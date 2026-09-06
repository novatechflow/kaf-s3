"""
Assertions about how Kafka and S3 actually behave.

Each test here pins an assumption that a released bug got wrong. A mock cannot
catch these, because a mock encodes the same assumption the code does.
"""
import gzip
import hashlib
import json
import os
import time

import pytest
from confluent_kafka import Consumer, KafkaException, Producer

from s3_connector import S3Consumer, S3Producer
from s3_connector.config import split_kafka_config

from .helpers import BUCKET, consume_one

pytestmark = pytest.mark.integration


def test_librdkafka_rejects_unknown_configuration_keys(kafka_bootstrap):
    """
    The reason dlq_topic must be split out of the client config. Shipped broken
    in v1.0.0 and invisible to tests that mock the client.
    """
    with pytest.raises(KafkaException, match="No such configuration property"):
        Producer({"bootstrap.servers": kafka_bootstrap, "dlq_topic": "dlq"})
    with pytest.raises(KafkaException, match="No such configuration property"):
        Consumer({"bootstrap.servers": kafka_bootstrap, "group.id": "g", "dlq_topic": "dlq"})

    client_config, dlq_topic = split_kafka_config(
        {"bootstrap.servers": kafka_bootstrap, "group.id": "g", "dlq_topic": "dlq"}
    )
    assert dlq_topic == "dlq"
    Producer(client_config)
    Consumer(client_config).close()


def test_single_put_etag_is_the_content_md5(producer_config, s3, topic):
    """The consumer's ETag check depends on this for sub-threshold objects."""
    payload = b"etag semantics " * 100
    producer = S3Producer(producer_config(max_inline_bytes=0, multipart_threshold=64 * 1024 * 1024))
    producer.produce(topic, payload)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    etag = s3.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')

    assert etag == hashlib.md5(payload).hexdigest()
    assert "-" not in etag


def test_multipart_etag_is_not_the_content_md5(producer_config, s3, topic):
    """
    Why the reference omits etag for multipart uploads and leans on sha256.
    A multipart ETag hashes the part hashes and carries a part-count suffix.
    """
    payload = os.urandom(6 * 1024 * 1024)
    producer = S3Producer(
        producer_config(max_inline_bytes=0, multipart_threshold=1024 * 1024)
    )
    producer.produce(topic, payload)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    etag = s3.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')

    assert etag != hashlib.md5(payload).hexdigest()
    assert "-" in etag


def test_gzip_output_is_stable_so_deterministic_keys_hold(producer_config, s3, topic):
    """
    gzip embeds an mtime by default. Re-producing an identical payload under a
    deterministic key used to rewrite the object with a different ETag, breaking
    the checksum on messages already in the topic.
    """
    payload = b"invoice batch " * 500
    options = {"max_inline_bytes": 0, "compression": "gzip", "deterministic_keys": True}

    producer = S3Producer(producer_config(**options))
    producer.produce(topic, payload)
    assert producer.close() == 0
    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    first_etag = s3.head_object(Bucket=BUCKET, Key=key)["ETag"]

    time.sleep(1.1)

    producer = S3Producer(producer_config(**options))
    producer.produce(topic, payload)
    assert producer.close() == 0
    second_etag = s3.head_object(Bucket=BUCKET, Key=key)["ETag"]

    assert first_etag == second_etag

    stored = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert gzip.decompress(stored) == payload


def test_ttl_seconds_writes_a_tag_a_lifecycle_rule_can_filter_on(producer_config, s3, topic):
    """
    Lifecycle rules filter on tags, never on user metadata, which is why the
    original ttl_epoch metadata drove nothing.
    """
    producer = S3Producer(producer_config(max_inline_bytes=0, ttl_seconds=86400))
    producer.produce(topic, b"expiring payload " * 100)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    tags = {
        t["Key"]: t["Value"]
        for t in s3.get_object_tagging(Bucket=BUCKET, Key=key)["TagSet"]
    }
    assert tags["kaf-s3-ttl-seconds"] == "86400"


def test_produce_applies_backpressure_rather_than_raising(producer_config, topic):
    """
    A queue smaller than the burst makes produce() raise BufferError unless the
    delivery reports are served. The retry loop is what keeps a burst alive.
    """
    config = producer_config(max_inline_bytes=1_000_000)
    config["kafka"]["queue.buffering.max.messages"] = 10
    config["s3"]["produce_timeout"] = 60.0

    producer = S3Producer(config)
    for i in range(200):
        producer.produce(topic, f"burst-{i}".encode())
    assert producer.close() == 0


def test_delete_after_consume_removes_the_object_only_after_commit(
    producer_config, consumer_config, s3, topic
):
    producer = S3Producer(producer_config(max_inline_bytes=0))
    producer.produce(topic, b"delete me " * 100)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]

    consumer = S3Consumer(consumer_config(
        kafka_overrides={"enable.auto.commit": False}, delete_after_consume=True,
    ))
    consumer.subscribe([topic])
    try:
        assert consume_one(consumer) is not None
        s3.head_object(Bucket=BUCKET, Key=key)  # still present before the commit

        consumer.commit()
        with pytest.raises(s3.exceptions.ClientError):
            s3.head_object(Bucket=BUCKET, Key=key)
    finally:
        consumer.close()


def test_delete_after_consume_refuses_auto_commit(consumer_config):
    with pytest.raises(ValueError, match="enable.auto.commit"):
        S3Consumer(consumer_config(delete_after_consume=True))


def test_dlq_receives_a_record_for_an_unreadable_reference(
    producer_config, consumer_config, kafka_bootstrap, s3, topic
):
    dlq_topic = f"{topic}-dlq"
    producer = S3Producer(producer_config(max_inline_bytes=0))
    producer.produce(topic, b"will be corrupted " * 50)
    assert producer.close() == 0

    contents = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    key = max(contents, key=lambda o: o["LastModified"])["Key"]
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"corrupted")

    config = consumer_config(kafka_overrides={"dlq_topic": dlq_topic})
    consumer = S3Consumer(config)
    consumer.subscribe([topic])
    try:
        with pytest.raises(Exception):
            consume_one(consumer)
    finally:
        consumer.close()

    reader = Consumer({
        "bootstrap.servers": kafka_bootstrap,
        "group.id": f"dlq-reader-{topic}",
        "auto.offset.reset": "earliest",
    })
    reader.subscribe([dlq_topic])
    try:
        deadline = time.time() + 60
        record = None
        while time.time() < deadline and record is None:
            msg = reader.poll(1.0)
            if msg is not None and not msg.error():
                record = json.loads(msg.value())
        assert record is not None, "no DLQ record arrived"
        assert "error" in record
        assert record["reference"]["s3_key"] == key
    finally:
        reader.close()


def test_rebalance_hands_uncommitted_messages_to_the_new_owner(
    producer_config, consumer_config, s3, multi_partition_topic
):
    """
    A second group member joining triggers a revoke. Deferred deletions for
    revoked partitions must be dropped so the new owner still finds the objects.
    """
    topic = multi_partition_topic
    producer = S3Producer(producer_config(max_inline_bytes=0))
    for i in range(6):
        producer.produce(topic, f"message-{i}".encode() * 100, key=str(i).encode())
    assert producer.close() == 0

    keys = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]}
    group = f"rebalance-{topic}"

    first = S3Consumer(consumer_config(
        group=group, kafka_overrides={"enable.auto.commit": False}, delete_after_consume=True,
    ))
    first.subscribe([topic])
    try:
        assert consume_one(first) is not None

        second = S3Consumer(consumer_config(
            group=group, kafka_overrides={"enable.auto.commit": False},
            delete_after_consume=True,
        ))
        second.subscribe([topic])
        try:
            # Drive both members so the group rebalances.
            for _ in range(20):
                first.poll(timeout=0.5)
                second.poll(timeout=0.5)
        finally:
            second.close()
    finally:
        first.close()

    remaining = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}
    # Nothing may vanish without a commit having authorised it.
    assert keys & remaining, "objects were deleted without a commit"
