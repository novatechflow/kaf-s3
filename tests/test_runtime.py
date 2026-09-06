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
