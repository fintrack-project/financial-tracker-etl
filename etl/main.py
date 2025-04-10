import importlib
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
from enum import Enum

class KafkaTopics(Enum):
    TRANSACTIONS_CONFIRMED = "TRANSACTIONS_CONFIRMED"
    PROCESS_TRANSACTIONS_TO_HOLDINGS = "PROCESS_TRANSACTIONS_TO_HOLDINGS"

TOPIC_TO_JOB_MAP = {
    KafkaTopics.TRANSACTIONS_CONFIRMED.value: "process_transactions_to_holdings",
}

def run_job(job_name):
    """
    Dynamically import and run a specific ETL job.
    """
    try:
        print(f"Attempting to import: etl.jobs.{job_name}")
        job_module = importlib.import_module(f"etl.jobs.{job_name}")
        print(f"Successfully imported: etl.jobs.{job_name}")
        job_module.run()
    except ModuleNotFoundError:
        print(f"Error: Job '{job_name}' not found.")
    except Exception as e:
        print(f"Error while running job '{job_name}': {e}")

def consume_kafka_messages():
    """
    Consume messages from Kafka and trigger the appropriate job.
    """
    consumer_config = {
        'bootstrap.servers': 'kafka:9093',  # Replace with your Kafka broker address
        'group.id': 'etl-job-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['TRANSACTION_CONFIRMED'])  # Add more topics if needed

    print("Kafka consumer started. Listening for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a 1-second timeout

            if msg is None:
                continue  # No message received, continue polling

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    # Handle other errors
                    print(f"Kafka error: {msg.error()}")
                    continue

            # Process the message
            topic = msg.topic()
            value = msg.value().decode('utf-8')
            print(f"Received message on topic '{topic}': {value}")

            # Trigger the appropriate job based on the topic
            if topic in TOPIC_TO_JOB_MAP:
                job_name = TOPIC_TO_JOB_MAP[topic]
                print(f"Triggering job: {job_name}")
                run_job(job_name)
            else:
                print(f"Unknown topic: {topic}")

    except KeyboardInterrupt:
        print("Kafka consumer interrupted.")
    finally:
        consumer.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "consume":
        consume_kafka_messages()
    else:
        print("Usage: python main.py consume")