"""
Shared constants and waiting helpers for the integration suite.
"""
import time

BUCKET = "kaf-s3-integration"


def wait_for(predicate, description, timeout=60.0, interval=0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001 - retry until the deadline
            last = exc
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {description} ({last})")


def consume_one(consumer, timeout=60.0):
    """
    Polls until a payload arrives or the deadline passes.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = consumer.poll(timeout=1.0)
        if payload is not None:
            return payload
    return None
