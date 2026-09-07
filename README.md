# Kafka S3 Connector (`kaf-s3-connector`)

[![CI](https://github.com/2pk03/kaf-s3/actions/workflows/ci.yml/badge.svg)](https://github.com/2pk03/kaf-s3/actions/workflows/ci.yml)
[![Docker](https://github.com/2pk03/kaf-s3/actions/workflows/docker.yml/badge.svg)](https://github.com/2pk03/kaf-s3/actions/workflows/docker.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Container](https://img.shields.io/badge/container-ghcr.io%2F2pk03%2Fkaf--s3--connector-blue)](https://ghcr.io/2pk03/kaf-s3-connector)

A Python library to seamlessly handle large Kafka messages by offloading them to Amazon S3.

This library provides a custom Kafka Producer and Consumer that automatically handle the process of storing large message payloads in an S3 bucket and passing lightweight references through Kafka.

## Key Features

-   **Automatic S3 Offloading:** Produce messages larger than Kafka's recommended limit without manual intervention.
-   **Transparent Consumption:** Consume large messages as if they were directly in Kafka.
-   **Data Integrity:** Verifies every S3 object against the SHA-256 and, where S3 provides a usable one, the ETag carried in the reference. References without a verifiable checksum are rejected by default (`require_integrity`).
-   **Secure by Default:** Leverages AWS IAM roles and the default `boto3` credential chain, avoiding the need to hardcode secrets.
-   **Flexible Configuration:** Built on top of `confluent-kafka-python`, allowing for full customization of Kafka client settings, including SASL and SSL.
-   **Operational Ready:** DLQ support, Prometheus `/metrics`, Helm chart, non-root image, optional compression, lifecycle-rule TTL tagging, SSE-KMS.
-   **Bounded Resources:** `max_payload_bytes` caps both the download and the gzip expansion, so a small object cannot inflate into an unbounded allocation.
-   **Tested Against Real Infrastructure:** an opt-in suite runs the connector against a real Kafka broker and a real S3 service in throwaway containers, on top of the mocked unit tests.

## Installation

```bash
pip install .
```

Requires Python 3.10 or newer.

## How it Works

A message is treated as an S3 reference only if it is a JSON object carrying both
`s3_bucket` and `s3_key`. Anything else — plain bytes, or JSON that is simply your own
payload — passes through untouched when `allow_inline_payloads` is enabled.

1.  The `S3Producer` receives a payload. Anything at or under `max_inline_bytes` is sent
    to Kafka unchanged and the rest of these steps do not apply.
2.  Larger payloads are uploaded to the configured bucket under a random key, or a
    content hash when `deterministic_keys` is set, optionally gzipped and encrypted.
3.  It produces a small JSON reference to the topic carrying the bucket, the key, a
    SHA-256 of the payload, the ETag where S3 provides a usable one, and the compression
    used. If the Kafka publish fails, the S3 object is rolled back.
4.  The `S3Consumer` recognises a reference by its `s3_bucket` and `s3_key` fields, and
    passes anything else through as an inline payload.
5.  It checks the reference against the configured bucket and prefix, downloads the
    object under a size cap, and decompresses it under the same cap.
6.  It verifies the payload against the SHA-256 and, where present, the ETag, then returns
    it. A reference carrying no verifiable checksum is rejected by default.

A multipart upload's ETag is a hash of part hashes rather than of the content, so those
references omit it and rely on the SHA-256 instead.

## Configuration

Configuration is handled through a single dictionary, with separate keys for `kafka` and `s3` settings.

### Security

-   **AWS Credentials:** This library is designed to be secure. **Do not** pass AWS credentials in the configuration. It uses `boto3`'s default credential discovery chain. The recommended and most secure way to provide credentials is by using an **IAM Role** attached to your compute instance (EC2, ECS, Lambda, etc.). Alternatively, you can use environment variables or a shared credentials file (`~/.aws/credentials`).
-   **Kafka Security:** All standard `confluent-kafka-python` security settings are supported and should be passed within the `kafka` dictionary. This includes SASL for authentication (e.g., `PLAIN`, `SCRAM`, and `GSSAPI` for **Kerberos**) and SSL/TLS for encryption.

### Example Configuration

```python
producer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
        # Optional DLQ for delivery failures
        # "dlq_topic": "my-s3-dlq",
    },
    "s3": {
        "bucket": "my-large-messages-bucket",
        # Optional toggles
        # "max_inline_bytes": 900_000,     # inline small payloads on Kafka, offload larger ones
        # "max_payload_bytes": 5 * 1024 * 1024 * 1024,  # hard cap on payload size
        # "prefix": "kafka/topic",         # prefix keys for organization/enforcement
        # "deterministic_keys": False,     # use payload hash for idempotent keys;
                                           #   marks references so consumers with
                                           #   delete_after_consume leave them alone
        # "multipart_threshold": 8388608,  # switch to multipart above this size;
                                           #   S3 caps a single PUT at 5 GiB
        # "produce_timeout": 30.0,         # how long to apply backpressure when
                                           #   librdkafka's local queue is full
        # "compression": "gzip",           # compress before S3 upload (gzip or None)
        # "ttl_seconds": 86400,            # tags the object kaf-s3-ttl-seconds=<n> so an
                                           #   S3 lifecycle rule can expire it
        # "server_side_encryption": "aws:kms", # SSE, optionally with KMS key below
        # "sse_kms_key_id": "<kms-key-id>",
        # "region_name": "eu-central-1",   # otherwise the boto3 default chain applies
        # "endpoint_url": "https://minio.local:9000",  # S3-compatible endpoints
    }
}

consumer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-s3-consumers",
        "auto.offset.reset": "earliest",
        # Optional: send failures to a DLQ topic
        # "dlq_topic": "my-s3-dlq",
    },
    "s3": {
        "bucket": "my-large-messages-bucket",
        # Optional toggles
        # "max_inline_bytes": 900_000,     # inline small payloads on Kafka, offload larger ones
        # "max_payload_bytes": 5 * 1024 * 1024 * 1024,  # hard cap on payload size
        # "delete_after_consume": False,   # single-consumer-group only; needs manual
                                           #   commits. See "Reclaiming S3 storage"
        # "allow_inline_payloads": True,   # allow non-reference payloads to pass through unchanged
        # "dlq_max_raw_bytes": 16384,      # raw bytes echoed into a DLQ record
        # "dlq_max_record_bytes": 524288,  # hard cap on the encoded DLQ record
        # "prefix": "kafka/topic",         # enforce prefix on incoming references
        # "require_integrity": True,       # reject references carrying no etag/sha256
        # "compression": "gzip",           # decompress automatically on consume
        # "deterministic_keys": False,     # use payload hash for idempotent keys;
                                           #   marks references so consumers with
                                           #   delete_after_consume leave them alone
        # "multipart_threshold": 8388608,  # switch to multipart above this size;
                                           #   S3 caps a single PUT at 5 GiB
        # "produce_timeout": 30.0,         # how long to apply backpressure when
                                           #   librdkafka's local queue is full (producer)
        # "ttl_seconds": 86400,            # tags objects for an S3 lifecycle rule (producer)
        # "server_side_encryption": "aws:kms", # SSE, optionally with KMS key below (producer)
        # "sse_kms_key_id": "<kms-key-id>",    # (producer)
    }
}
```

## Usage

### Producer

```python
from s3_connector import S3Producer

# Initialize producer with the config
producer = S3Producer(config=producer_config)

# Read a sample file
with open("examples/sample_payload.txt", "rb") as f:
    payload_data = f.read()

# Produce the data to a topic
producer.produce(topic="large-messages-topic", payload=payload_data)

# produce() is asynchronous. Flush before exiting or queued messages are lost.
producer.flush(timeout=30.0)
producer.close()
print("Produced message to Kafka via S3.")
```

`S3Producer` is also a context manager, which closes (and therefore flushes) on exit:

```python
with S3Producer(config=producer_config) as producer:
    producer.produce(topic="large-messages-topic", payload=payload_data)
```

### Consumer

```python
from s3_connector import S3Consumer

# Initialize consumer with the config
consumer = S3Consumer(config=consumer_config)
consumer.subscribe(["large-messages-topic"])
# subscribe() also accepts on_assign / on_revoke / on_lost rebalance callbacks.

print("Waiting for messages...")
while True:
    try:
        # Poll for a message
        payload = consumer.poll(timeout=10.0)

        if payload:
            print(f"Received message of size: {len(payload)} bytes")
            # Save the received file
            with open("examples/received_payload.txt", "wb") as f:
                f.write(payload)
            break # Exit after one message for this example

    except KeyboardInterrupt:
        break

consumer.close()
```

`poll()` returns `None` both when no message arrived and when a message was
skipped as unusable. Register the `skipped` hook to tell the two apart:

```python
consumer_config["hooks"] = {
    "skipped": lambda reason, data: print(f"dropped a message: {reason}"),
}
```

### Threads

A connector instance is not safe to share across threads. Give each thread its own
`S3Producer` or `S3Consumer`, which is also what `confluent-kafka` expects for consumers.

### Reclaiming S3 storage

Offloaded objects outlive the Kafka messages that reference them, so something has to
remove them. There are two mechanisms, and they are not equivalent.

**S3 lifecycle rules (recommended).** Expiry belongs to the bucket, not to a consumer. A
rule works no matter how many consumer groups read the topic, keeps working when a
consumer is down, and costs nothing at runtime. Set `ttl_seconds` on the producer and the
object is tagged `kaf-s3-ttl-seconds=<n>`; lifecycle rules can filter on tags, so a rule
can then expire it. `config/s3-lifecycle.json` is ready to apply:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-large-messages-bucket \
  --lifecycle-configuration file://config/s3-lifecycle.json
```

Set the expiry longer than the topic's retention, or a consumer that falls behind will
find its objects gone. S3 expiry is day-granular, so objects live a little past their TTL.
The file also carries a prefix-based rule and an `AbortIncompleteMultipartUpload` rule,
which is worth enabling on any bucket this library writes to.

**`delete_after_consume` (narrow).** Deletes each object once its message is consumed.
It reclaims storage immediately rather than a day later, which matters at high volume,
but it is **only correct when exactly one consumer group reads the topic**. Kafka is a
fan-out log: any other group still needing that object loses the message, and no consumer
can detect that another group exists. The connector logs a warning at startup, and that
is the most it can do.

It also requires `enable.auto.commit: False` and is refused otherwise — under auto-commit
the object is removed before the payload is processed, so a crash loses the record.

Given a choice, prefer the lifecycle rule.

#### How deferred deletion behaves

Deletion waits for the commit that makes the offset durable, so the object always outlives
the offset:

- deletions are tracked per partition and offset, so `commit(message=...)` deletes only
  what that commit covers;
- a commit with deletions pending is forced synchronous, since an asynchronous commit has
  not reached the broker yet;
- partitions lost to a rebalance drop their pending deletions, so the member that takes
  them over still finds the objects;
- objects written with `deterministic_keys` are never deleted, because deduplication means
  several messages can reference one object;
- `close()` drops anything uncommitted rather than deleting it.

For at-least-once delivery, disable auto-commit and commit after processing:

```python
consumer_config["kafka"]["enable.auto.commit"] = False
...
payload = consumer.poll(timeout=10.0)
if payload:
    handle(payload)
    consumer.commit()
```

## Testing

Unit tests mock Kafka and S3 and need nothing running:

```bash
pip install ".[test]"
pytest
```

Integration tests run the connector against a real Kafka broker and a real S3
service, started as throwaway containers. They need Docker, are marked
`integration`, and are excluded from the default run:

```bash
pip install ".[integration]"
pytest -m integration
```

They exist because a mock encodes the same assumptions the code does, so it
cannot catch a wrong one. Each test in `tests/integration/test_real_semantics.py`
pins a behaviour that a released bug got wrong — multipart ETags are not content
hashes, gzip embeds an mtime, librdkafka rejects unknown configuration keys,
`produce()` raises `BufferError` under load — alongside end-to-end coverage of
deferred deletion, rebalance handover, DLQ publication, tampering and missing
objects. Without Docker, or without the extra installed, the suite skips.

## Docker & Operations

Build locally:

```bash
docker build -t kaf-s3-connector:local .
```

Pull from GHCR:

```bash
docker pull ghcr.io/2pk03/kaf-s3-connector:latest
# or a specific tag (git tag or commit SHA)
docker pull ghcr.io/2pk03/kaf-s3-connector:v1.0.0
```

Run as consumer (replace envs as needed):

```bash
docker run --rm -p 8000:8000 \
  -e MODE=consumer \
  -e TOPIC=large-messages-topic \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e KAFKA_GROUP_ID=my-group \
  -e S3_BUCKET=my-large-messages-bucket \
  kaf-s3-connector:local
```

Run as producer reading stdin:

```bash
echo "hello" | docker run --rm -i \
  -e MODE=producer \
  -e TOPIC=large-messages-topic \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e S3_BUCKET=my-large-messages-bucket \
  kaf-s3-connector:local
```

Configuration is driven by env vars:
- Kafka: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID` (consumer), `DLQ_TOPIC`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_SSL_*`, etc.
- S3: `S3_BUCKET`, `S3_PREFIX`, `S3_DELETE_AFTER_CONSUME`, `S3_ALLOW_INLINE_PAYLOADS`, `S3_MAX_INLINE_BYTES`, `S3_MAX_PAYLOAD_BYTES`, `S3_DETERMINISTIC_KEYS`, `S3_COMPRESSION` (`gzip`), `S3_TTL_SECONDS`, `S3_SSE`, `S3_SSE_KMS_KEY_ID`.
- App: `MODE` (`producer`|`consumer`), `TOPIC`, `POLL_TIMEOUT`, `METRICS_PORT` (default 8000; the image healthcheck follows it), `METRICS_ADDRESS` (default all interfaces), `LOG_LEVEL`.
- Also: `AWS_REGION`, `S3_ENDPOINT_URL`, `S3_REQUIRE_INTEGRITY`, `KAFKA_ENABLE_AUTO_COMMIT`.
- Boolean env vars accept `true`/`1`/`yes`/`on` (case-insensitive); anything else is false.
- With `KAFKA_ENABLE_AUTO_COMMIT=false` the container commits after each payload, giving at-least-once delivery.

### Metrics
- `/metrics` exposes Prometheus text format on `METRICS_PORT` (default 8000).
- Only low-cardinality labels (`topic`, `partition`, `reason`) become series. Payload sizes are aggregated into `<event>_bytes` counters rather than one series per size, and S3 keys are never used as labels. `reason` is always a fixed code, never a formatted message.
- Skip reason `object_missing` covers objects that are gone: a TTL lifecycle rule fired, another consumer group deleted them, or an auto-committed message is being replayed. Other S3 errors (`AccessDenied`, `NoSuchBucket`, throttling) propagate to the caller.
- Integrity reasons: `etag_mismatch`, `sha256_mismatch`, `missing_checksum`, `missing_etag`, `prefix_violation`, `unexpected_bucket`, `object_too_large`, `decompressed_too_large`, `decompression_failed`, `unsupported_compression`. Skip reasons: `tombstone`, `malformed_message`, `missing_key`.
- The endpoint is unauthenticated. Keep it on an internal network, or bind it explicitly with `METRICS_ADDRESS`.
- Sample Prometheus scrape config: `config/prometheus.yml`
- Sample Grafana dashboard JSON: `config/grafana-dashboard.json`
- Sample S3 lifecycle policy: `config/s3-lifecycle.json`

### Helm
- Chart: `charts/kaf-s3-connector`
- Override values (examples):

```bash
helm upgrade --install kaf-s3 charts/kaf-s3-connector \
  --set image.repository=ghcr.io/2pk03/kaf-s3-connector \
  --set image.tag=latest \
  --set env.MODE=consumer \
  --set env.TOPIC=large-messages-topic \
  --set env.KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  --set env.KAFKA_GROUP_ID=my-group \
  --set env.S3_BUCKET=my-large-messages-bucket
```

`service.port` drives the Service, container port, both probes, the scrape annotation and
the app's `METRICS_PORT` — set it alone to move the metrics endpoint, and do not put
`METRICS_PORT` in `env`.

Kafka SASL/SSL passwords belong in a Secret, not in `values.env`, which renders into the
Deployment in plain text:

```bash
kubectl create secret generic kaf-s3-kafka-credentials \
  --from-literal=KAFKA_SASL_USERNAME=svc-kaf-s3 \
  --from-literal=KAFKA_SASL_PASSWORD=...

helm upgrade --install kaf-s3 charts/kaf-s3-connector \
  --set 'envFrom[0].secretRef.name=kaf-s3-kafka-credentials'
```

The chart sets `prometheus.io/scrape` pod annotations; `config/prometheus.yml` includes a
matching Kubernetes discovery job.

## Case Study

This connector was architected as part of a consulting engagement to solve a common Apache Kafka® challenge: handling large messages without destabilizing the cluster.

**Full case study:** [Kafka-to-S3 Connector: Large Message Offloading and Scalable ETL](https://www.novatechflow.com/p/kafka-to-s3-connector-large-message.html)

### The Problem

Kafka is designed for high-throughput streaming but has strict limits on message sizes. When workloads produce large payloads—documents, JSON blobs, binary exports, telemetry batches—teams face a choice: raise broker limits (risky), chunk messages (complex), or find a better pattern.

### The Solution

`kaf-s3-connector` offloads large payloads to S3 while keeping Kafka responsible only for lightweight references:
```
Producer                          Consumer
   │                                 │
   ▼                                 ▼
┌─────────┐    S3 key + ETag    ┌─────────┐
│ Payload │ ──────────────────► │  Kafka  │
│  (big)  │                     │  topic  │
└────┬────┘                     └────┬────┘
     │                               │
     ▼                               ▼
┌─────────┐                     ┌─────────┐
│   S3    │ ◄─────────────────► │ Fetch & │
│ bucket  │    retrieve blob    │ verify  │
└─────────┘                     └─────────┘
```

### Benefits

- **Removes message size limits** — payloads up to 5GB
- **Stabilizes Kafka clusters** — brokers never see large messages
- **Enables lakehouse integration** — S3 objects ready for Iceberg, Spark, Flink
- **Production-ready** — DLQ, compression, ETag integrity, Prometheus metrics, Helm chart

### Use Cases

- Ingesting files, documents, or binary data into streaming pipelines
- Pre-processing before writing to Apache Iceberg® or Parquet
- Integrating Kafka with S3-based data lakes

---

**Building data pipelines with Kafka, S3, or lakehouse architectures?**

→ [Consulting Services](https://www.novatechflow.com/p/consulting-services.html)  
→ [Book a call](https://cal.com/alexanderalten)

## License

MIT. See [LICENSE](LICENSE).
