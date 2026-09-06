"""
Helpers for separating connector settings from raw librdkafka client settings.
"""

CONNECTOR_ONLY_KAFKA_KEYS = ("dlq_topic",)


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
