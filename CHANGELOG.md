# Changelog

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
