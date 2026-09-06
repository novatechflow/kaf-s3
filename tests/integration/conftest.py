"""
Fixtures backing the integration suite with a real Kafka broker and a real
S3 implementation.

Every other test in this repository mocks boto3 and confluent_kafka, so they
verify our understanding of those APIs rather than the APIs themselves. Several
released bugs were exactly that kind of mistake: multipart ETags are not content
hashes, gzip embeds an mtime, librdkafka rejects unknown configuration keys, and
produce() raises BufferError under load. These fixtures exist so those
assumptions are checked against the real thing.
"""
import os
import uuid

import pytest

from .helpers import BUCKET, wait_for

try:
    import testcontainers  # noqa: F401

    TESTCONTAINERS_AVAILABLE = True
except ImportError:  # pragma: no cover - the suite is skipped without the extra
    TESTCONTAINERS_AVAILABLE = False

collect_ignore_glob = [] if TESTCONTAINERS_AVAILABLE else ["test_*.py"]


def _docker_is_available():
    try:
        from testcontainers.core.docker_client import DockerClient

        DockerClient().client.ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def _require_docker():
    if not _docker_is_available():
        pytest.skip("Docker is not available on this host")


@pytest.fixture(scope="session")
def kafka_bootstrap():
    from testcontainers.community.kafka import KafkaContainer

    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


@pytest.fixture(scope="session")
def s3_endpoint():
    """
    Starts MinIO, exports its credentials, and creates the test bucket.

    The connector deliberately takes no credentials in its config and relies on
    boto3's default chain, so exporting them here also exercises that decision.
    """
    import boto3
    from testcontainers.community.minio import MinioContainer

    # MinIO answers SSE-S3 (AES256) only when a KMS key is configured; without one
    # it returns NotImplemented, which would look like a connector bug.
    container = MinioContainer().with_env(
        "MINIO_KMS_SECRET_KEY",
        "kaf-s3-test-key:MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDA=",
    )
    with container:
        config = container.get_config()
        os.environ["AWS_ACCESS_KEY_ID"] = config["access_key"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = config["secret_key"]
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        endpoint = f"http://{config['endpoint']}"

        client = boto3.client("s3", endpoint_url=endpoint)
        wait_for(lambda: bool(client.list_buckets()), "MinIO to accept requests")
        client.create_bucket(Bucket=BUCKET)
        yield endpoint


@pytest.fixture
def s3(s3_endpoint):
    import boto3

    return boto3.client("s3", endpoint_url=s3_endpoint)


def _create_topic(bootstrap, partitions):
    from confluent_kafka.admin import AdminClient, NewTopic

    name = f"it-{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[
        name
    ].result(timeout=30)
    wait_for(lambda: name in admin.list_topics(timeout=10).topics, f"topic {name}")
    return name


@pytest.fixture
def topic(kafka_bootstrap):
    """
    A fresh topic per test, created explicitly rather than relying on broker
    auto-creation.
    """
    return _create_topic(kafka_bootstrap, 1)


@pytest.fixture
def multi_partition_topic(kafka_bootstrap):
    return _create_topic(kafka_bootstrap, 2)


@pytest.fixture
def producer_config(kafka_bootstrap, s3_endpoint):
    def build(**s3_overrides):
        return {
            "kafka": {"bootstrap.servers": kafka_bootstrap},
            "s3": dict({"bucket": BUCKET, "endpoint_url": s3_endpoint}, **s3_overrides),
        }

    return build


@pytest.fixture
def consumer_config(kafka_bootstrap, s3_endpoint):
    def build(group=None, kafka_overrides=None, **s3_overrides):
        kafka = {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": group or f"g-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
        kafka.update(kafka_overrides or {})
        return {
            "kafka": kafka,
            "s3": dict({"bucket": BUCKET, "endpoint_url": s3_endpoint}, **s3_overrides),
        }

    return build
