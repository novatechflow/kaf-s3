import json
import pytest
from s3_connector import S3Producer

@pytest.fixture
def producer_config():
    return {
        "kafka": {
            "bootstrap.servers": "mock:9092",
            "test.mock.num.brokers": 3
        },
        "s3": {"bucket": "test-bucket", "max_inline_bytes": 0}
    }

def test_producer_init(mocker, producer_config):
    """Tests the initialization of the S3Producer."""
    mocker.patch("s3_connector.producer.boto3")
    mocker.patch("s3_connector.producer.Producer")
    
    producer = S3Producer(producer_config)
    
    assert producer.s3_bucket == "test-bucket"
    assert producer.kafka_producer is not None
    assert producer.s3_client is not None

def test_produce_message(mocker, producer_config):
    """Tests the produce method."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value

    # Mock the S3 response
    mock_boto3.client.return_value.put_object.return_value = {"ETag": '"12345"'}

    producer = S3Producer(producer_config)
    
    payload = b"This is a test payload"
    topic = "test-topic"
    
    producer.produce(topic, payload)

    # Assert S3 call
    mock_boto3.client.return_value.put_object.assert_called_once()
    args, kwargs = mock_boto3.client.return_value.put_object.call_args
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Body"] == payload
    
    # Assert Kafka call
    mock_kafka_producer_instance.produce.assert_called_once()
    args, kwargs = mock_kafka_producer_instance.produce.call_args
    assert args[0] == topic
    
    # Verify the content of the Kafka message
    ref_message = json.loads(kwargs["value"].decode('utf-8'))
    assert ref_message["s3_bucket"] == "test-bucket"
    assert ref_message["etag"] == "12345"
    assert ref_message["sha256"]
    assert ref_message["compression"] is None
    assert "s3_key" in ref_message

    mock_kafka_producer_instance.poll.assert_called_once_with(0)

def test_produce_inline_message(mocker):
    """Ensures small payloads are sent directly to Kafka."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value

    config = {
        "kafka": {
            "bootstrap.servers": "mock:9092",
            "test.mock.num.brokers": 3
        },
        "s3": {"bucket": "test-bucket", "max_inline_bytes": 10},
    }

    producer = S3Producer(config)
    
    payload = b"small"
    topic = "test-topic"
    
    producer.produce(topic, payload)

    # S3 is not called for inline payloads
    mock_boto3.client.return_value.put_object.assert_not_called()

    mock_kafka_producer_instance.produce.assert_called_once_with(topic, key=None, value=payload)
    mock_kafka_producer_instance.poll.assert_called_once_with(0)

def test_produce_cleanup_on_error(mocker, producer_config):
    """Ensures S3 uploads are cleaned up if Kafka produce raises."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value
    mock_kafka_producer_instance.produce.side_effect = RuntimeError("fail")
    mock_boto3.client.return_value.put_object.return_value = {"ETag": '"12345"'}

    producer = S3Producer(producer_config)
    payload = b"This is a test payload"

    with pytest.raises(RuntimeError):
        producer.produce("test-topic", payload)

    mock_boto3.client.return_value.delete_object.assert_called_once()


def test_produce_cleanup_on_delivery_failure(mocker, producer_config):
    """Ensures S3 uploads are cleaned up when delivery callback receives an error."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value
    mock_boto3.client.return_value.put_object.return_value = {"ETag": '"12345"'}

    def produce_side_effect(*args, **kwargs):
        # Simulate delivery callback with an error
        on_delivery = kwargs["on_delivery"]
        on_delivery(Exception("delivery-fail"), mocker.Mock())
    mock_kafka_producer_instance.produce.side_effect = produce_side_effect

    producer = S3Producer(producer_config)
    payload = b"This is a test payload"

    producer.produce("test-topic", payload)

    mock_boto3.client.return_value.delete_object.assert_called_once()


def test_producer_payload_too_large(mocker):
    """Raises when payload exceeds max_payload_bytes."""
    mocker.patch("s3_connector.producer.boto3")
    mocker.patch("s3_connector.producer.Producer")
    config = {
        "kafka": {
            "bootstrap.servers": "mock:9092",
            "test.mock.num.brokers": 3
        },
        "s3": {"bucket": "test-bucket", "max_inline_bytes": 0, "max_payload_bytes": 5},
    }
    producer = S3Producer(config)
    with pytest.raises(ValueError):
        producer.produce("topic", b"123456")  # 6 bytes


def test_producer_config_validation(mocker):
    """Validates max_inline_bytes and max_payload_bytes config."""
    mocker.patch("s3_connector.producer.boto3")
    mocker.patch("s3_connector.producer.Producer")

    bad_config_inline_gt_payload = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "test-bucket", "max_inline_bytes": 10, "max_payload_bytes": 5},
    }
    with pytest.raises(ValueError):
        S3Producer(bad_config_inline_gt_payload)

    bad_config_negative = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "test-bucket", "max_inline_bytes": -1},
    }
    with pytest.raises(ValueError):
        S3Producer(bad_config_negative)


def test_produce_with_compression(mocker, producer_config):
    """Ensures gzip compression flag and compressed payload are used."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value

    mock_boto3.client.return_value.put_object.return_value = {"ETag": '"etag"'}

    cfg = dict(producer_config)
    cfg["s3"]["compression"] = "gzip"
    producer = S3Producer(cfg)

    payload = b"hello"
    producer.produce("topic", payload)

    # Stored payload should be compressed
    args, kwargs = mock_boto3.client.return_value.put_object.call_args
    assert kwargs["Body"] != payload

    raw_value = mock_kafka_producer_instance.produce.call_args.kwargs["value"]
    ref_message = json.loads(raw_value)
    assert ref_message["compression"] == "gzip"


def test_build_key_with_prefix_and_deterministic(mocker):
    """Key generation respects prefix and determinism."""
    mocker.patch("s3_connector.producer.boto3")
    mocker.patch("s3_connector.producer.Producer")
    cfg = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "b", "prefix": "pfx", "deterministic_keys": True, "max_inline_bytes": 0},
    }
    producer = S3Producer(cfg)
    key1 = producer._build_s3_key(b"payload")
    key2 = producer._build_s3_key(b"payload")
    assert key1 == key2
    assert key1.startswith("pfx/")


def test_dlq_topic_is_not_passed_to_librdkafka(mocker):
    """dlq_topic is a connector setting; librdkafka rejects unknown properties."""
    mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")

    cfg = {
        "kafka": {"bootstrap.servers": "mock:9092", "dlq_topic": "my-dlq"},
        "s3": {"bucket": "test-bucket"},
    }
    producer = S3Producer(cfg)

    assert "dlq_topic" not in mock_kafka_producer_class.call_args.args[0]
    assert producer.dlq_topic == "my-dlq"


def test_deterministic_keys_are_not_deleted_on_delivery_failure(mocker):
    """A shared deterministic key may still back a delivered message."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_instance = mock_kafka_producer_class.return_value
    mock_boto3.client.return_value.put_object.return_value = {"ETag": '"12345"'}

    def produce_side_effect(*args, **kwargs):
        kwargs["on_delivery"](Exception("delivery-fail"), mocker.Mock())
    mock_kafka_producer_instance.produce.side_effect = produce_side_effect

    cfg = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "test-bucket", "max_inline_bytes": 0, "deterministic_keys": True},
    }
    producer = S3Producer(cfg)
    producer.produce("test-topic", b"This is a test payload")

    mock_boto3.client.return_value.delete_object.assert_not_called()


def test_flush_and_close(mocker, producer_config):
    """Callers can block until delivery instead of losing queued messages."""
    mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_class.return_value.flush.return_value = 0

    producer = S3Producer(producer_config)
    assert producer.flush(1.0) == 0
    mock_kafka_producer_class.return_value.flush.assert_called_once_with(1.0)

    producer.close(2.0)
    assert mock_kafka_producer_class.return_value.flush.call_args.args == (2.0,)


def test_context_manager_closes_producer(mocker, producer_config):
    """The context manager flushes on exit."""
    mocker.patch("s3_connector.producer.boto3")
    mock_kafka_producer_class = mocker.patch("s3_connector.producer.Producer")
    mock_kafka_producer_class.return_value.flush.return_value = 0

    with S3Producer(producer_config):
        pass

    mock_kafka_producer_class.return_value.flush.assert_called_once()


def test_s3_client_uses_region_and_endpoint(mocker):
    """region_name and endpoint_url reach the boto3 client instead of being ignored."""
    mock_boto3 = mocker.patch("s3_connector.producer.boto3")
    mocker.patch("s3_connector.producer.Producer")

    cfg = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {
            "bucket": "test-bucket",
            "region_name": "eu-central-1",
            "endpoint_url": "https://minio.local:9000",
        },
    }
    S3Producer(cfg)

    mock_boto3.client.assert_called_once_with(
        "s3", region_name="eu-central-1", endpoint_url="https://minio.local:9000"
    )
