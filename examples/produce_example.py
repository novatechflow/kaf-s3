from s3_connector import S3Producer

# NOTE: You must have Kafka and S3 (or a compatible service like MinIO)
# running and properly configured for this example to work.

# Example Configuration
producer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
    },
    "s3": {
        # Replace with your S3 bucket name
        "bucket": "my-large-messages-bucket",
    }
}

def main():
    """
    Reads a file and produces its content to a Kafka topic using S3 offloading.
    """
    try:
        producer = S3Producer(config=producer_config)
    except Exception as e:
        print(f"Failed to initialize producer: {e}")
        return

    file_path = "examples/sample_payload.txt"
    topic = "large-messages-topic"

    try:
        with open(file_path, "rb") as f:
            payload = f.read()
        
        print(f"Producing message from '{file_path}' to topic '{topic}'...")
        producer.produce(topic=topic, payload=payload)

        print("Message queued; flushing before exit.")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        undelivered = producer.close(timeout=30.0)
        if undelivered:
            print(f"{undelivered} message(s) could not be delivered.")
        else:
            print("Message delivered successfully.")

if __name__ == "__main__":
    main()
