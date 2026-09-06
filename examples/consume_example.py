from s3_connector import S3Consumer, DataIntegrityError

# NOTE: You must have Kafka and S3 (or a compatible service like MinIO)
# running and properly configured for this example to work.

# Example Configuration
consumer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-s3-consumers",
        "auto.offset.reset": "earliest",
    },
    "s3": {
        # Replace with your S3 bucket name
        "bucket": "my-large-messages-bucket",
    }
}

def main():
    """
    Consumes messages from a Kafka topic, downloads the payload from S3,
    and saves it to a file.
    """
    try:
        consumer = S3Consumer(config=consumer_config)
    except Exception as e:
        print(f"Failed to initialize consumer: {e}")
        return

    topic = "large-messages-topic"
    consumer.subscribe([topic])
    output_file_path = "examples/received_payload.txt"

    print(f"Waiting for messages on topic '{topic}'...")
    try:
        while True:
            try:
                payload = consumer.poll(timeout=10.0)
                if payload:
                    print(f"Message received. Payload size: {len(payload)} bytes.")
                    with open(output_file_path, "wb") as f:
                        f.write(payload)
                    print(f"Payload saved to '{output_file_path}'.")
                    break  # Exit after processing one message
            except DataIntegrityError as e:
                print(f"Error: {e}")
            except KeyboardInterrupt:
                print("Consumer interrupted.")
                break
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
