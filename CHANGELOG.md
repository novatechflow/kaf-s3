# Changelog

## v1.8.0

### Fixed
- `commit(message=...)` deleted every deferred S3 object, not just those the commit
  covered. Committing one partition's message removed objects belonging to other
  partitions and to later offsets that were still in flight; a crash then lost them,
  because their offsets had never been committed. Deferred deletions are now tracked per
  partition and offset, and a message commit drains only that partition up to that offset.
- A consumer group rebalance stranded the new partition owner. `subscribe()` registered no
  rebalance callbacks, so partitions taken away still had deferred deletions queued, and
  the next commit deleted objects for messages another member was about to reprocess.
  Revoked and lost partitions now drop their pending deletions.

Both are reachable only with `delete_after_consume` and manual commits — the combination
the README recommends for at-least-once delivery — and only in a multi-consumer group,
which is the normal deployment.

### Changed
- `subscribe()` accepts `on_assign`, `on_revoke` and `on_lost`. The connector registers its
  own listeners either way and calls yours after its own bookkeeping.

## v1.7.0

### Fixed
- `deterministic_keys` combined with `compression: "gzip"` rejected valid messages.
  `gzip.compress` embeds an mtime, so re-producing an identical payload wrote different
  bytes to the same key; any earlier message still referencing it then failed its ETag
  check and was rejected as corrupt. Compression is now byte-reproducible (`mtime=0`).
- `deterministic_keys` combined with `delete_after_consume` silently lost messages.
  Deduplicated messages share one object, so consuming the first deleted it and every
  other message pointing at it was skipped as `object_missing` — a loss made silent by
  the v1.6.0 change that stopped missing objects from raising. Producers now mark such
  references `deterministic`, and consumers do not delete objects that carry the mark.

### Changed
- Added a producer backpressure panel to the Grafana dashboard. `produce_queue_full` was
  emitted from v1.6.0 but had nowhere to show; every metric the code emits is now on the
  dashboard.
- Documented that a connector instance is not safe to share across threads.

## v1.6.0

### Fixed
- A missing S3 object raised a raw `botocore` `ClientError` out of `poll()`, killing the
  consumer. Objects go missing for ordinary reasons — a `ttl_seconds` lifecycle rule
  fired, another consumer group ran with `delete_after_consume`, or an auto-committed
  message is being replayed — so the message is now skipped with reason `object_missing`.
  Credential, bucket and throttling errors still propagate to the caller.
- `produce()` raised `BufferError` as soon as librdkafka's local queue filled, discarding
  the S3 object it had just uploaded. Produce now drains delivery reports and retries for
  up to `produce_timeout` (30 s) before giving up, applying backpressure instead of
  failing a burst.
- Inline messages were produced without a delivery callback, so their delivery failures
  were silent — no log line, no `produce_error` metric. They are now reported like
  offloaded messages; there is simply no S3 object to roll back.
- The image healthcheck probed a hardcoded port 8000 while `METRICS_PORT` is configurable,
  so any other port left the container permanently unhealthy. It now reads `METRICS_PORT`.

### Changed
- The chart had three places to set the port — `service.port`, `env.METRICS_PORT` and the
  scrape annotation. Setting only `service.port` moved the Service, container port and
  both probes while the app kept listening on 8000, producing a liveness restart loop.
  `service.port` is now the single source of truth for all four.

## v1.5.0

### Fixed (data loss)
- Inline JSON objects were silently dropped. Any payload small enough to stay on Kafka
  that happened to be a JSON object was parsed as an S3 reference, found to name no
  bucket or key, and skipped — even with `allow_inline_payloads` at its default of True.
  A message is now treated as a reference only when it carries both `s3_bucket` and
  `s3_key` as strings; everything else is an ordinary inline payload.
- `commit()` drained pending S3 deletions immediately after an asynchronous commit, which
  has not yet reached the broker. A crash in that window left the offset uncommitted and
  the object already deleted, defeating the deferral added in v1.4.0. Commits are now
  forced synchronous whenever deletions are pending.

### Fixed
- An unset `S3_BUCKET` arrives as `""`, and the check tested for the key rather than a
  value, so the connector started with an empty bucket and rejected every reference. Empty
  and whitespace-only buckets are now rejected at construction on both sides.
- DLQ records had no size bound. A large malformed message was echoed whole, and since
  JSON escaping expands raw bytes several-fold, the record could exceed the broker's
  message limit and fail to publish. Records are now bounded by measuring the encoded
  size (`dlq_max_raw_bytes`, `dlq_max_record_bytes`).
- Oversized objects are rejected from the `ContentLength` response header instead of
  transferring up to `max_payload_bytes` before discarding them.

### Added
- `KAFKA_ENABLE_AUTO_COMMIT` env var, and the container's consumer loop commits after each
  payload when auto-commit is off. At-least-once delivery was previously reachable through
  the library API only; from the container, offsets would simply never have advanced.
- Boolean environment variables share one parser and now accept `1`, `yes` and `on`
  alongside `true`, instead of each call site testing `== "true"`.

## v1.4.0

### Fixed (security)
- Integrity failures used the full error message as a Prometheus label, and those
  messages embed the S3 key. This reintroduced through `consume_integrity_error` the
  unbounded label cardinality that v1.3.0 removed elsewhere: 50 distinct keys produced
  50 series. Reasons are now fixed codes (`etag_mismatch`, `sha256_mismatch`,
  `missing_checksum`, `prefix_violation`, `unexpected_bucket`, `object_too_large`,
  `decompressed_too_large`, `decompression_failed`, `unsupported_compression`,
  `missing_etag`), and the key stays in the exception and DLQ record only.
- A `prefix` of `"/"` collapsed to an empty string, silently disabling prefix
  enforcement. Now rejected at construction on both the producer and the consumer.
- `sse_kms_key_id` was accepted without `server_side_encryption: "aws:kms"`, sending a
  KMS key id that S3 ignores, so objects were not KMS-encrypted as intended. Both this
  and unknown SSE modes are now rejected.

### Fixed (correctness)
- `delete_after_consume` removed the S3 object before `poll()` returned, so a crash
  between poll and processing lost the record from both Kafka and S3. With
  `enable.auto.commit: False` deletion is now deferred until `commit()`; uncommitted
  deletes are dropped on `close()`. Auto-commit keeps the old behaviour and now logs a
  warning about the window.
- Payloads above 8 MiB (configurable via `multipart_threshold`) upload via multipart
  instead of a single PUT, which S3 caps at 5 GiB. Multipart references carry no `etag`,
  since a multipart ETag is not a content checksum; SHA-256 still covers those.
- Broker errors raise `KafkaException` rather than a bare `Exception`.
- `/metrics` groups samples by family and emits `# TYPE`. Interleaved families are
  invalid exposition and strict parsers reject them.

### Changed
- Rewrote the Grafana dashboard. The old one plotted raw counters with `sum()` instead of
  `rate()` and contained an invalid expression (`increase(consume_inline[5m]*0)`, which
  multiplies a range vector). Seven panels now cover produce/consume rate and throughput,
  integrity failures by reason, skips, and delivery outcomes.
- `config/prometheus.yml` gained a Kubernetes pod-discovery job matching the scrape
  annotations the chart now sets.
- Helm: pod annotations for Prometheus discovery, and `envFrom` so SASL and SSL passwords
  come from a Secret rather than plaintext in values.
- Every GitHub release since v1.0.0 shipped with empty notes: the extraction step split
  the changelog on `^## ` after prepending `"## "`, so it always took the empty string
  before the delimiter. The logic now lives in `.github/scripts/release_notes.py`, is
  covered by tests, and fails the release when the tag does not match the top CHANGELOG
  section rather than publishing nothing.

## v1.3.0

### Fixed (blocking)
- Container and Helm deployments were no-ops: `runtime.py` defined `run()` but had no
  `__main__` guard, so `python -m s3_connector.runtime` exited 0 immediately. Added a
  `__main__` module, a `__main__` guard, and a `kaf-s3-connector` console script, which is
  now the image entrypoint.
- Setting `dlq_topic` (or `DLQ_TOPIC`) crashed both clients at construction: the key was
  passed straight to librdkafka, which rejects unknown properties. Connector-only settings
  are now split out of the client config.
- `S3Consumer.poll()` raised on tombstones (`AttributeError`), binary inline payloads
  (`UnicodeDecodeError`) and non-object JSON (`TypeError`) instead of skipping them. A
  single malformed record could kill every consumer in the group; the runtime loop now
  also survives unexpected errors.

### Fixed (security)
- Prometheus label values are escaped, and only `topic`, `partition` and `reason` become
  series. S3 keys and payload sizes are no longer labels, removing both a metrics
  injection vector and an unbounded memory leak. Sizes are aggregated into
  `<event>_bytes` counters.
- Integrity verification is mandatory by default: a reference carrying neither `etag` nor
  `sha256` is now rejected rather than trusted. Opt out with `require_integrity: False`.
- `max_payload_bytes` is enforced on the consumer, bounding both the S3 download and the
  gzip expansion, so a small object can no longer inflate into an unbounded allocation.
- Multi-stage Docker build; the compiler and `librdkafka-dev` no longer ship in the final
  image. Added `.dockerignore` so `.git`, `dist/` and local files stay out of image layers.
- Metrics bind address is configurable via `METRICS_ADDRESS`.

### Fixed (correctness)
- Added `S3Producer.flush()`, `close()` and context-manager support. Without them there
  was no way to guarantee delivery, and queued messages were silently dropped at exit.
- `S3Consumer.close()` now flushes pending DLQ records before shutting down.
- Delivery-failure cleanup no longer deletes the S3 object when `deterministic_keys` is
  enabled, where the same key may back an already-delivered message.
- `AWS_REGION` / `region_name` reached no client and was silently ignored; it is now
  applied, alongside a new `endpoint_url` for S3-compatible stores.
- `requires-python` corrected to `>=3.10`; the code has used PEP 604 syntax since v1.0.0.
- Metrics server uses `ThreadingHTTPServer`, so one slow scraper can no longer stall the
  endpoint and trigger liveness-probe restarts.
- Added `S3Consumer.commit()` and a `skipped` hook, so callers can run at-least-once and
  can distinguish an empty poll from a dropped message.

### Changed
- Helm chart ships pod/container security contexts, default resource requests and limits,
  and pins the image tag to the chart `appVersion` instead of `latest`.
- CI runs on pushes to `main`, across Python 3.10-3.13, and now builds the image, smoke
  tests the entrypoint against `/metrics`, and lints/renders the Helm chart.
- `_version.py` is no longer tracked in git; `.DS_Store` is ignored.

## v1.2.5
- Add version validation to PyPI workflow to prevent dev version uploads
- Note: v1.2.3 and v1.2.4 dev versions were incorrectly published due to setuptools-scm detecting commits after tags

## v1.2.4
- Fix setuptools-scm configuration to remove local version identifiers for PyPI compatibility

## v1.2.3
- Fix PyPI metadata configuration (add [project] section to pyproject.toml)
- Implement setuptools-scm for fully automated version management from git tags
- No manual version updates needed - version now derived automatically from git tags
- Update PyPI workflow to support setuptools-scm with full git history

## v1.2.2
- Version bump for release with setuptools pin and PyPI checks.

## v1.2.1
- Pin setuptools<75 for builds (avoid Metadata-Version 2.4 issues) and add twine check in PyPI workflow.
- Workflow triggers limited to tags or manual dispatch (CI/Docker/Release/PyPI).
- Multi-arch Docker builds (amd64, arm64) on tags.

## v1.1.0
- Packaging/metadata fixes to support PyPI trusted publishing flow.
- Docker pushes target ghcr.io/2pk03/kaf-s3-connector with tagged images.
- Added badges, issue templates, contributing guide, Helm chart, metrics samples, hardened non-root image.

## v1.0.1
- Fixed packaging metadata and release automation for PyPI/GitHub releases.
- Docker pushes now target ghcr.io/2pk03/kaf-s3-connector; GHCR pull instructions added.
- Added badges, issue templates, and contributing guide for better adoption.
- Added Helm chart, metrics samples, and hardened non-root image.

## v1.0.0
- Hardened non-root Docker image with healthcheck and Prometheus `/metrics`.
- Env-driven runtime entrypoint for producer/consumer, DLQ + metrics hooks.
- Added gzip compression, deterministic keys, TTL metadata, SSE/SSE-KMS support.
- DLQ handling on producer delivery errors and consumer integrity failures.
- Optional inline handling, bucket/prefix enforcement, SHA-256 + ETag integrity checks.
- Helm chart for deployment, Prometheus scrape config, Grafana dashboard sample.
- GitHub Actions for tests and Docker build/push.
