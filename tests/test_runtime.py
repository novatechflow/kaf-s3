import urllib.error
import urllib.request

import pytest

from s3_connector.runtime import (
    MetricsRegistry,
    build_config_from_env,
    metric_hook,
    run,
    start_metrics_server,
)


def test_metrics_registry_render():
    reg = MetricsRegistry()
    reg.inc("produce_offloaded", {"topic": "t"})
    reg.inc("produce_offloaded", {"topic": "t"}, 2)
    reg.inc("consume_success")
    rendered = reg.render()
    assert "produce_offloaded{topic=\"t\"} 3" in rendered
    assert "consume_success 1" in rendered


def test_build_config_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "k:9092")
    monkeypatch.setenv("KAFKA_GROUP_ID", "g1")
    monkeypatch.setenv("S3_BUCKET", "bkt")
    monkeypatch.setenv("S3_PREFIX", "pfx")
    monkeypatch.setenv("S3_TTL_SECONDS", "60")
    monkeypatch.setenv("S3_COMPRESSION", "gzip")
    monkeypatch.setenv("DLQ_TOPIC", "dlq")

    cfg = build_config_from_env()
    assert cfg["kafka"]["bootstrap.servers"] == "k:9092"
    assert cfg["kafka"]["group.id"] == "g1"
    assert cfg["kafka"]["dlq_topic"] == "dlq"
    assert cfg["s3"]["bucket"] == "bkt"
    assert cfg["s3"]["prefix"] == "pfx"
    assert cfg["s3"]["ttl_seconds"] == 60
    assert cfg["s3"]["compression"] == "gzip"
    assert cfg["s3"]["dlq_topic"] == "dlq"


def test_render_escapes_label_values():
    """Label values must not be able to break the exposition format."""
    reg = MetricsRegistry()
    reg.inc("consume_success", {"topic": 'a"b\\c'})
    rendered = reg.render()
    assert 'topic="a\\"b\\\\c"' in rendered


def test_metric_hook_bounds_label_cardinality():
    """High-cardinality fields become counters, not one series per value."""
    reg = MetricsRegistry()
    hook = metric_hook(reg)
    for i in range(100):
        hook("consume_success", {"topic": "t", "bytes": 10, "s3_key": f"key-{i}"})

    assert len(reg._counters) == 2
    rendered = reg.render()
    assert 'consume_success{topic="t"} 100' in rendered
    assert 'consume_success_bytes{topic="t"} 1000' in rendered
    assert "s3_key" not in rendered


def test_runtime_module_has_entrypoint():
    """python -m s3_connector must actually start the runtime."""
    import s3_connector.__main__ as entry
    assert entry.run is run


def test_run_rejects_unknown_mode(monkeypatch):
    monkeypatch.setenv("TOPIC", "t")
    monkeypatch.setenv("MODE", "nonsense")
    with pytest.raises(ValueError):
        run()


def test_run_requires_topic(monkeypatch):
    monkeypatch.delenv("TOPIC", raising=False)
    with pytest.raises(ValueError):
        run()


def test_metrics_server_serves_metrics():
    """The metrics endpoint binds and answers a real HTTP request."""
    reg = MetricsRegistry()
    reg.inc("consume_success", {"topic": "t"})
    srv = start_metrics_server(reg, 0, "127.0.0.1")
    try:
        port = srv.server_address[1]
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            assert resp.status == 200
            assert 'consume_success{topic="t"} 1' in resp.read().decode()
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/other", timeout=5)
    finally:
        srv.shutdown()
        srv.server_close()


def test_render_groups_families_and_declares_type():
    """Prometheus requires one TYPE per family with its samples grouped together."""
    reg = MetricsRegistry()
    reg.inc("consume_success", {"topic": "b"})
    reg.inc("consume_integrity_error", {"reason": "etag_mismatch"})
    reg.inc("consume_success", {"topic": "a"})

    lines = reg.render().strip().splitlines()
    assert lines == [
        "# TYPE consume_integrity_error counter",
        'consume_integrity_error{reason="etag_mismatch"} 1',
        "# TYPE consume_success counter",
        'consume_success{topic="a"} 1',
        'consume_success{topic="b"} 1',
    ]


def test_auto_commit_can_be_configured_from_env(monkeypatch):
    """The container needs a way to opt into at-least-once delivery."""
    monkeypatch.setenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
    assert build_config_from_env()["kafka"]["enable.auto.commit"] is False

    monkeypatch.setenv("KAFKA_ENABLE_AUTO_COMMIT", "true")
    assert build_config_from_env()["kafka"]["enable.auto.commit"] is True

    monkeypatch.delenv("KAFKA_ENABLE_AUTO_COMMIT")
    assert "enable.auto.commit" not in build_config_from_env()["kafka"]


@pytest.mark.parametrize("value,expected", [
    ("true", True), ("TRUE", True), ("1", True), ("yes", True), ("on", True),
    ("false", False), ("0", False), ("", False), ("nonsense", False),
])
def test_boolean_env_vars_share_one_parser(monkeypatch, value, expected):
    monkeypatch.setenv("S3_DELETE_AFTER_CONSUME", value)
    assert build_config_from_env()["s3"]["delete_after_consume"] is expected


def test_unset_bucket_is_an_empty_string(monkeypatch):
    """Documents the shape the consumer now rejects instead of accepting."""
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert build_config_from_env()["s3"]["bucket"] == ""


def test_consumer_loop_commits_when_auto_commit_is_off(mocker):
    """Without this the manual-commit container never advances its offsets."""
    from s3_connector.runtime import _run_consumer

    consumer = mocker.MagicMock()
    consumer.auto_commit = False
    consumer.poll.side_effect = [b"payload", b"payload", KeyboardInterrupt()]
    mocker.patch("s3_connector.runtime.S3Consumer", return_value=consumer)

    _run_consumer({}, "topic", MetricsRegistry())

    assert consumer.commit.call_count == 2
    assert consumer.commit.call_args.kwargs == {"asynchronous": False}
    consumer.close.assert_called_once()


def test_consumer_loop_does_not_commit_under_auto_commit(mocker):
    from s3_connector.runtime import _run_consumer

    consumer = mocker.MagicMock()
    consumer.auto_commit = True
    consumer.poll.side_effect = [b"payload", KeyboardInterrupt()]
    mocker.patch("s3_connector.runtime.S3Consumer", return_value=consumer)

    _run_consumer({}, "topic", MetricsRegistry())

    consumer.commit.assert_not_called()


def test_consumer_loop_does_not_commit_an_empty_poll(mocker):
    """commit() with nothing consumed would raise; skipped messages ride along
    with the next successful commit."""
    from s3_connector.runtime import _run_consumer

    consumer = mocker.MagicMock()
    consumer.auto_commit = False
    consumer.poll.side_effect = [None, None, KeyboardInterrupt()]
    mocker.patch("s3_connector.runtime.S3Consumer", return_value=consumer)

    _run_consumer({}, "topic", MetricsRegistry())

    consumer.commit.assert_not_called()
