class DataIntegrityError(Exception):
    """
    Raised when a payload fetched from S3 cannot be trusted: the reference does
    not match the stored object, carries no verifiable checksum, points outside
    the configured bucket or prefix, or exceeds the configured size limit.
    """
    pass
