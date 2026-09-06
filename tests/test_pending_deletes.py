from s3_connector.pending_deletes import PendingDeletes


class _Partition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition


def _loaded():
    pending = PendingDeletes()
    pending.add("t", 0, 10, "b", "p0-o10")
    pending.add("t", 0, 11, "b", "p0-o11")
    pending.add("t", 1, 5, "b", "p1-o5")
    return pending


def test_empty_is_falsy():
    assert not PendingDeletes()
    assert len(PendingDeletes()) == 0


def test_len_counts_across_partitions():
    assert len(_loaded()) == 3


def test_release_all_empties_the_queue():
    pending = _loaded()
    assert sorted(pending.release_all()) == [
        ("b", "p0-o10"), ("b", "p0-o11"), ("b", "p1-o5"),
    ]
    assert not pending


def test_release_through_is_scoped_to_partition_and_offset():
    pending = _loaded()
    assert pending.release_through("t", 0, 10) == [("b", "p0-o10")]
    assert len(pending) == 2
    assert pending.release_through("t", 0, 11) == [("b", "p0-o11")]
    assert pending.release_through("t", 1, 5) == [("b", "p1-o5")]
    assert not pending


def test_release_through_an_unknown_partition_releases_nothing():
    pending = _loaded()
    assert pending.release_through("t", 7, 100) == []
    assert len(pending) == 3


def test_discard_drops_revoked_partitions_only():
    pending = _loaded()
    pending.discard([_Partition("t", 1)])
    assert sorted(pending.release_all()) == [("b", "p0-o10"), ("b", "p0-o11")]


def test_discard_of_an_unheld_partition_is_a_no_op():
    pending = _loaded()
    pending.discard([_Partition("other", 0)])
    assert len(pending) == 3
