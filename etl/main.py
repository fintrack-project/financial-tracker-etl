import importlib
import sys
import os
import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from enum import Enum
from etl.utils import log_message
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class ConsumerKafkaTopics(Enum):
    """
    Kafka topics that the ETL system consumes messages from.
    """
    TRANSACTIONS_CONFIRMED = "TRANSACTIONS_CONFIRMED"
    MARKET_DATA_UPDATE_REQUEST = "MARKET_DATA_UPDATE_REQUEST"
    MARKET_INDEX_DATA_UPDATE_REQUEST = "MARKET_INDEX_DATA_UPDATE_REQUEST"
    HISTORICAL_MARKET_DATA_REQUEST = "HISTORICAL_MARKET_DATA_REQUEST"
    HOLDINGS_MONTHLY_REQUEST = "HOLDINGS_MONTHLY_REQUEST"


class ProducerKafkaTopics(Enum):
    """
    Kafka topics that the ETL system produces messages to.
    """
    MARKET_DATA_UPDATE_COMPLETE = "MARKET_DATA_UPDATE_COMPLETE"
    MARKET_INDEX_DATA_UPDATE_COMPLETE = "MARKET_INDEX_DATA_UPDATE_COMPLETE"
    HISTORICAL_MARKET_DATA_COMPLETE = "HISTORICAL_MARKET_DATA_COMPLETE"
    PROCESS_TRANSACTIONS_TO_HOLDINGS_COMPLETE = "PROCESS_TRANSACTIONS_TO_HOLDINGS_COMPLETE"
    PROCESS_TRANSACTIONS_TO_HOLDINGS_MONTHLY_COMPLETE = "PROCESS_TRANSACTIONS_TO_HOLDINGS_MONTHLY_COMPLETE"

# Updated TOPIC_TO_JOB_MAP structure
TOPIC_TO_JOB_MAP = {
    ConsumerKafkaTopics.TRANSACTIONS_CONFIRMED.value: {
        "jobs": [
            {"job_name": "process_transactions_to_holdings", "requires_params": True},
            {"job_name": "process_transactions_to_holdings_monthly", "requires_params": True}
        ]
    },
    ConsumerKafkaTopics.MARKET_DATA_UPDATE_REQUEST.value: {
        "jobs": [
            {"job_name": "fetch_market_data", "requires_params": True}
        ]
    },
    ConsumerKafkaTopics.MARKET_INDEX_DATA_UPDATE_REQUEST.value: {
        "jobs": [
            {"job_name": "fetch_market_index_data", "requires_params": True}
        ]
    },
    ConsumerKafkaTopics.HISTORICAL_MARKET_DATA_REQUEST.value: {
        "jobs": [
            {"job_name": "fetch_historical_market_data", "requires_params": True}
        ]
    },
    ConsumerKafkaTopics.HOLDINGS_MONTHLY_REQUEST.value: {
        "jobs": [
            {"job_name": "process_transactions_to_holdings_monthly", "requires_params": False}
        ]
    }
}

def run_job(job_name, params=None):
    """
    Dynamically import and run a specific ETL job.
    If params are provided, pass them to the job's run() function.
    """
    try:
        log_message(f"Attempting to import: etl.jobs.{job_name}")
        job_module = importlib.import_module(f"etl.jobs.{job_name}")
        log_message(f"Successfully imported: etl.jobs.{job_name}")

        if params:
            log_message(f"Attempting to run the 'run()' function in {job_name} with parameters: {params}")
            job_module.run(params)
        else:
            log_message(f"Attempting to run the 'run()' function in {job_name} without parameters")
            job_module.run()

        log_message(f"Successfully ran the 'run()' function in {job_name}")
    except ModuleNotFoundError:
        log_message(f"Error: Job '{job_name}' not found.")
    except AttributeError as e:
        log_message(f"Error: 'run()' function not found in job '{job_name}'. Details: {e}")
    except Exception as e:
        log_message(f"Error while running job '{job_name}': {e}")

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
    consumer.subscribe([topic.value for topic in ConsumerKafkaTopics])  # Add more topics if needed

    log_message("Kafka consumer started. Listening for messages...")

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
                    log_message(f"Kafka error: {msg.error()}")
                    continue

            # Process the message
            topic = msg.topic()
            value = msg.value().decode('utf-8')
            log_message(f"Received message on topic '{topic}': {value}")

            # Handle empty content
            if not value.strip():
                log_message(f"Warning: Received empty message on topic '{topic}'. Skipping processing.")
                continue

            # Trigger the appropriate jobs based on the topic
            if topic in TOPIC_TO_JOB_MAP:
                job_configs = TOPIC_TO_JOB_MAP[topic]["jobs"]

                for job_config in job_configs:
                    job_name = job_config["job_name"]
                    requires_params = job_config["requires_params"]

                    log_message(f"Triggering job: {job_name}")

                    if requires_params:
                        try:
                            message_payload = json.loads(value)
                            run_job(job_name, message_payload)
                        except json.JSONDecodeError:
                            log_message(f"Error: Failed to decode JSON message on topic '{topic}'. Skipping job '{job_name}'.")
                            continue
                    else:
                        run_job(job_name)
            else:
                log_message(f"Unknown topic: {topic}")

    except KeyboardInterrupt:
        log_message("Kafka consumer interrupted.")
    finally:
        consumer.close()

def publish_kafka_messages(topic, params=None):
    """
    Publish a message to a Kafka topic.
    
    Args:
        topic (ProducerKafkaTopics): The Kafka topic to publish the message to.
        params (dict, optional): The message payload to be serialized as JSON.
    """
    log_message(f"Publishing Kafka message to topic: {topic.value}...")

    producer_config = {
        'bootstrap.servers': 'kafka:9093',  # Replace with your Kafka broker address
    }
    producer = Producer(producer_config)

    try:
        # Serialize the message payload as JSON
        message = json.dumps(params) if params else "{}"
        producer.produce(topic.value, key="key", value=message)
        producer.flush()
        log_message(f"Successfully published message to topic: {topic.value}")
    except Exception as e:
        log_message(f"Error publishing Kafka message to topic {topic.value}: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "consume":
        consume_kafka_messages()
    else:
        log_message("Usage: python3 -m etl.main consume")