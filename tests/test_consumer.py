import json
import pytest
from unittest.mock import MagicMock
from s3_connector import S3Consumer, DataIntegrityError

@pytest.fixture
def consumer_config():
    return {
        "kafka": {
            "bootstrap.servers": "mock:9092",
            "group.id": "test-group",
            "test.mock.num.brokers": 3
        },
        "s3": {"bucket": "test-bucket", "delete_after_consume": True}
    }

def test_consumer_init(mocker, consumer_config):
    """Tests the initialization of the S3Consumer."""
    mocker.patch("s3_connector.consumer.boto3")
    mocker.patch("s3_connector.consumer.Consumer")
    
    consumer = S3Consumer(consumer_config)
    
    assert consumer.kafka_consumer is not None
    assert consumer.s3_client is not None

def test_poll_message(mocker, consumer_config):
    """Tests polling and successfully retrieving a message."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    # Mock Kafka message
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "12345",
        "sha256": "7add08d874756f51da6f92958e09e7596f4bbeea9ea6e8a4e0da23fd76b62917"
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    # Mock S3 response
    s3_payload = b"This is the payload from S3"
    mock_s3_response = {
        "Body": MagicMock(),
        "ETag": '"12345"'
    }
    mock_s3_response["Body"].read.return_value = s3_payload
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == s3_payload
    mock_boto3.client.return_value.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")
    mock_boto3.client.return_value.delete_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")

def test_poll_data_integrity_error(mocker, consumer_config):
    """Tests that DataIntegrityError is raised on ETag mismatch."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    # Mock Kafka message with a different ETag
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "expected-etag",
        "sha256": "expected-sha"
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    # Mock S3 response with a mismatched ETag
    mock_s3_response = {"ETag": '"actual-etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"payload-mismatch"
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    
    with pytest.raises(DataIntegrityError):
        consumer.poll()

def test_poll_inline_payload(mocker, consumer_config):
    """Tests returning inline payloads when JSON parsing fails."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = b"raw-payload"
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == b"raw-payload"


def test_poll_malformed_rejected_when_inline_disabled(mocker, consumer_config):
    """Malformed payload returns None when inline pass-through is disabled."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = b"raw-payload"
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    cfg = json.loads(json.dumps(consumer_config))  # shallow clone
    cfg["s3"]["allow_inline_payloads"] = False
    consumer = S3Consumer(cfg)
    payload = consumer.poll()

    assert payload is None


def test_poll_unexpected_bucket(mocker, consumer_config):
    """Raises on unexpected bucket."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    ref_message = {
        "s3_bucket": "other-bucket",
        "s3_key": "test-key",
        "sha256": "deadbeef",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    consumer = S3Consumer(consumer_config)
    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_consumer_config_validation(mocker, consumer_config):
    """Ensures required config keys are validated."""
    mocker.patch("s3_connector.consumer.boto3")
    mocker.patch("s3_connector.consumer.Consumer")

    bad_cfg_missing_group = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "test-bucket"},
    }
    with pytest.raises(ValueError):
        S3Consumer(bad_cfg_missing_group)

    bad_cfg_missing_bucket = {
        "kafka": {"bootstrap.servers": "mock:9092", "group.id": "g"},
        "s3": {},
    }
    with pytest.raises(ValueError):
        S3Consumer(bad_cfg_missing_bucket)


def test_consume_gzip_payload(mocker, consumer_config):
    """Compressed payloads are decompressed after download."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    import gzip as _gzip
    import hashlib as _hashlib
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "compression": "gzip",
        "sha256": _hashlib.sha256(b"test").hexdigest(),
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    compressed = _gzip.compress(b"test")
    mock_s3_response = {"ETag": '"etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = compressed
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == b"test"


def test_consume_prefix_enforced(mocker, consumer_config):
    """Rejects keys outside configured prefix."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "wrong/key",
        "sha256": "deadbeef",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["prefix"] = "allowed"
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_consume_dlq_on_integrity_error(mocker, consumer_config):
    """Publishes to DLQ on integrity errors."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value
    mock_dlq_producer_class = mocker.patch("s3_connector.consumer.Producer")
    mock_dlq_producer_instance = mock_dlq_producer_class.return_value

    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "expected-etag",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    mock_s3_response = {"ETag": '"actual-etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"payload"
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    cfg = json.loads(json.dumps(consumer_config))
    cfg["kafka"]["dlq_topic"] = "dlq"
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()

    mock_dlq_producer_instance.produce.assert_called_once()


def _msg(value):
    m = MagicMock()
    m.value.return_value = value
    m.error.return_value = None
    m.topic.return_value = "test-topic"
    return m


@pytest.mark.parametrize("value", [None, b"\xff\xfe\x00", b"123", b'["a"]'])
def test_poll_never_crashes_on_unparseable_payload(mocker, consumer_config, value):
    """Tombstones, binary payloads and non-object JSON must not raise."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_consumer_class.return_value.poll.return_value = _msg(value)

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["allow_inline_payloads"] = False
    consumer = S3Consumer(cfg)

    assert consumer.poll() is None


def test_poll_binary_inline_payload_passes_through(mocker, consumer_config):
    """Binary payloads survive the inline path instead of failing to decode."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_consumer_class.return_value.poll.return_value = _msg(b"\xff\xfe\x00")

    consumer = S3Consumer(consumer_config)

    assert consumer.poll() == b"\xff\xfe\x00"


def test_skip_hook_distinguishes_skip_from_empty_poll(mocker, consumer_config):
    """The 'skipped' hook fires for dropped messages but not for empty polls."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_consumer_class.return_value.poll.return_value = _msg(None)

    skips = []
    cfg = json.loads(json.dumps(consumer_config))
    cfg["hooks"] = {"skipped": lambda reason, data: skips.append(reason)}
    consumer = S3Consumer(cfg)

    assert consumer.poll() is None
    assert skips == ["tombstone"]

    mock_consumer_class.return_value.poll.return_value = None
    assert consumer.poll() is None
    assert skips == ["tombstone"]


def test_reference_without_checksum_is_rejected(mocker, consumer_config):
    """Integrity metadata is mandatory by default, not opt-in per message."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    ref_message = {"s3_bucket": "test-bucket", "s3_key": "test-key"}
    mock_consumer_class.return_value.poll.return_value = _msg(
        json.dumps(ref_message).encode("utf-8")
    )

    consumer = S3Consumer(consumer_config)

    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_reference_without_checksum_allowed_when_opted_out(mocker, consumer_config):
    """require_integrity=False restores the permissive legacy behaviour."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    ref_message = {"s3_bucket": "test-bucket", "s3_key": "test-key"}
    mock_consumer_class.return_value.poll.return_value = _msg(
        json.dumps(ref_message).encode("utf-8")
    )

    mock_s3_response = {"ETag": '"etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"payload"
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["require_integrity"] = False
    consumer = S3Consumer(cfg)

    assert consumer.poll() == b"payload"


def test_decompression_bomb_is_capped(mocker, consumer_config):
    """A small gzip object cannot expand past max_payload_bytes."""
    import gzip as gz
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "compression": "gzip",
        "sha256": "unused",
    }
    mock_consumer_class.return_value.poll.return_value = _msg(
        json.dumps(ref_message).encode("utf-8")
    )

    bomb = gz.compress(b"\x00" * 10_000_000)
    assert len(bomb) < 100_000

    mock_s3_response = {"ETag": '"etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = bomb
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["max_payload_bytes"] = 1024
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_oversized_object_is_rejected(mocker, consumer_config):
    """Stored objects larger than max_payload_bytes are refused."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    ref_message = {"s3_bucket": "test-bucket", "s3_key": "test-key", "sha256": "unused"}
    mock_consumer_class.return_value.poll.return_value = _msg(
        json.dumps(ref_message).encode("utf-8")
    )

    mock_s3_response = {"ETag": '"etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"x" * 2048
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["max_payload_bytes"] = 1024
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_dlq_topic_is_not_passed_to_librdkafka(mocker, consumer_config):
    """dlq_topic is a connector setting; librdkafka rejects unknown properties."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_producer_class = mocker.patch("s3_connector.consumer.Producer")

    cfg = json.loads(json.dumps(consumer_config))
    cfg["kafka"]["dlq_topic"] = "dlq"
    S3Consumer(cfg)

    assert "dlq_topic" not in mock_consumer_class.call_args.args[0]
    assert "dlq_topic" not in mock_producer_class.call_args.args[0]


def test_close_flushes_dlq_producer(mocker, consumer_config):
    """Pending DLQ records are flushed before the consumer shuts down."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_producer_class = mocker.patch("s3_connector.consumer.Producer")
    mock_producer_class.return_value.flush.return_value = 0

    cfg = json.loads(json.dumps(consumer_config))
    cfg["kafka"]["dlq_topic"] = "dlq"
    consumer = S3Consumer(cfg)
    consumer.close()

    mock_producer_class.return_value.flush.assert_called_once()
    mock_consumer_class.return_value.close.assert_called_once()


def test_s3_client_uses_region_and_endpoint(mocker, consumer_config):
    """region_name and endpoint_url reach the boto3 client instead of being ignored."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mocker.patch("s3_connector.consumer.Consumer")

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["region_name"] = "eu-central-1"
    cfg["s3"]["endpoint_url"] = "https://minio.local:9000"
    S3Consumer(cfg)

    mock_boto3.client.assert_called_once_with(
        "s3", region_name="eu-central-1", endpoint_url="https://minio.local:9000"
    )
