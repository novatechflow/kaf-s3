"""
Helpers for separating connector settings from raw librdkafka client settings.
"""

CONNECTOR_ONLY_KAFKA_KEYS = ("dlq_topic",)


def parse_bool(value, default=False):
    """
    Interprets a config value that may be a bool, or a librdkafka-style or
    environment-style string.
    """
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def split_kafka_config(kafka_config):
    """
    Splits a user-supplied Kafka section into a librdkafka client config and the
    connector-only settings that librdkafka would reject as unknown properties.

    :return: (client_config, dlq_topic)
    """
    client_config = {
        key: value
        for key, value in kafka_config.items()
        if key not in CONNECTOR_ONLY_KAFKA_KEYS
    }
    return client_config, kafka_config.get("dlq_topic")


def require_bucket(s3_config):
    """
    Returns the configured bucket, rejecting a missing or empty value.

    An empty string is the shape an unset S3_BUCKET environment variable takes,
    and it would otherwise start a connector that rejects every reference.
    """
    bucket = s3_config.get("bucket")
    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError("S3 bucket must be specified in the configuration.")
    if bucket != bucket.strip():
        raise ValueError("S3 bucket must not have leading or trailing whitespace.")
    return bucket


def build_s3_client(boto3_module, s3_config):
    """
    Builds an S3 client honouring the optional region and endpoint overrides.
    """
    client_kwargs = {}
    if s3_config.get("region_name"):
        client_kwargs["region_name"] = s3_config["region_name"]
    if s3_config.get("endpoint_url"):
        client_kwargs["endpoint_url"] = s3_config["endpoint_url"]
    return boto3_module.client("s3", **client_kwargs)
