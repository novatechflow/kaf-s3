"""
Bookkeeping for S3 deletions deferred until their Kafka offsets are committed.
"""
import logging

logger = logging.getLogger(__name__)


class PendingDeletes:
    """
    Holds deletions per (topic, partition) with the offset that authorises them.

    A deletion may only happen once its message's offset is committed: until
    then the message can be replayed, either by this consumer after a crash or
    by another group member after a rebalance, and the object must still exist.
    """

    def __init__(self):
        self._by_partition = {}

    def __len__(self):
        return sum(len(entries) for entries in self._by_partition.values())

    def __bool__(self):
        return bool(self._by_partition)

    def add(self, topic, partition, offset, bucket, key):
        self._by_partition.setdefault((topic, partition), []).append((offset, bucket, key))

    def release_all(self):
        """
        Returns every deletion, for a commit that advanced all assigned partitions.
        """
        released = [entry[1:] for entries in self._by_partition.values() for entry in entries]
        self._by_partition = {}
        return released

    def release_through(self, topic, partition, offset):
        """
        Returns the deletions a commit of one message authorises: that partition
        only, and only up to the committed offset.
        """
        entries = self._by_partition.get((topic, partition), [])
        released, kept = [], []
        for entry_offset, bucket, key in entries:
            if entry_offset <= offset:
                released.append((bucket, key))
            else:
                kept.append((entry_offset, bucket, key))
        if kept:
            self._by_partition[(topic, partition)] = kept
        else:
            self._by_partition.pop((topic, partition), None)
        return released

    def discard(self, partitions):
        """
        Forgets deletions for partitions this consumer no longer owns. Their
        messages are uncommitted, so another member will consume them and must
        still find the objects.
        """
        for part in partitions:
            dropped = self._by_partition.pop((part.topic, part.partition), None)
            if dropped:
                logger.info(
                    "Dropping %s deferred S3 deletion(s) for revoked %s[%s].",
                    len(dropped), part.topic, part.partition,
                )
