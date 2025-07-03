import importlib
import sys
import os
import json
import time
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from enum import Enum
from etl.utils import log_message, load_env_variables
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
try:
    env_vars = load_env_variables()
except FileNotFoundError:
    # If .env file doesn't exist, use environment variables directly
    env_vars = os.environ

# Get Kafka broker from environment variables
KAFKA_BROKER = env_vars.get('KAFKA_BROKER')
if not KAFKA_BROKER:
    raise ValueError("KAFKA_BROKER environment variable is not set")

log_message(f"Using Kafka broker: {KAFKA_BROKER}")

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
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'etl-job-consumer-group',
        'client.id': f'etl-consumer-{os.getpid()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 60000,
        'heartbeat.interval.ms': 20000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 100,  # Reduced from 500ms for faster response
        'max.partition.fetch.bytes': 1048576,
        'check.crcs': True,
        'retries': 3,
        'retry.backoff.ms': 1000,
        # Optimized for better performance
        'fetch.max.wait.ms': 100,  # Maximum time to wait for data
        'fetch.min.bytes': 1,  # Minimum bytes to fetch
        'max.partition.fetch.bytes': 1048576,  # 1MB per partition
        'receive.buffer.bytes': 32768,  # 32KB receive buffer
        'send.buffer.bytes': 131072,  # 128KB send buffer
    }

    # Log consumer configuration
    log_message("Consumer configuration:")
    for key, value in consumer_config.items():
        log_message(f"  {key}: {value}")

    try:
        consumer = Consumer(consumer_config)
        topics = [topic.value for topic in ConsumerKafkaTopics]
        log_message(f"Attempting to subscribe to topics: {topics}")
        consumer.subscribe(topics)
        log_message(f"Kafka consumer started. Listening for messages on broker: {KAFKA_BROKER}")
        log_message(f"Successfully subscribed to topics: {topics}")
    except Exception as e:
        log_message(f"Failed to initialize Kafka consumer: {e}")
        raise

    try:
        while True:
            log_message("Polling for messages...")
            msg = consumer.poll(0.1)  # Poll for messages with a 100ms timeout for faster response

            if msg is None:
                # Don't log every poll to reduce noise
                continue  # No message received, continue polling

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    log_message(f"Reached end of partition for topic {msg.topic()}, partition {msg.partition()}")
                    continue
                else:
                    # Handle other errors
                    log_message(f"Kafka error: {msg.error()}")
                    continue

            try:
                # Process the message
                topic = msg.topic()
                value = msg.value().decode('utf-8')
                log_message(f"Received message on topic '{topic}': {value}")
                log_message(f"Message metadata - Partition: {msg.partition()}, Offset: {msg.offset()}, Timestamp: {msg.timestamp()}")

                # Handle empty content
                if not value.strip():
                    log_message(f"Warning: Received empty message on topic '{topic}'. Skipping processing.")
                    continue

                # Trigger the appropriate jobs based on the topic
                if topic in TOPIC_TO_JOB_MAP:
                    log_message(f"Found job configuration for topic '{topic}'")
                    job_configs = TOPIC_TO_JOB_MAP[topic]["jobs"]
                    log_message(f"Job configurations for topic '{topic}': {job_configs}")
                    processing_successful = True

                    for job_config in job_configs:
                        job_name = job_config["job_name"]
                        requires_params = job_config["requires_params"]
                        log_message(f"Processing job: {job_name}, requires_params: {requires_params}")

                        try:
                            if requires_params:
                                log_message(f"Attempting to parse JSON payload for job {job_name}")
                                message_payload = json.loads(value)
                                log_message(f"Successfully parsed JSON payload: {message_payload}")
                                log_message(f"Starting job {job_name} with parameters")
                                run_job(job_name, message_payload)
                            else:
                                log_message(f"Starting job {job_name} without parameters")
                                run_job(job_name)
                            log_message(f"Successfully completed job {job_name}")
                        except json.JSONDecodeError as e:
                            log_message(f"Error: Failed to decode JSON message on topic '{topic}'. Error: {str(e)}")
                            processing_successful = False
                            break
                        except Exception as e:
                            log_message(f"Error while running job '{job_name}': {str(e)}")
                            log_message(f"Error details: {type(e).__name__}: {str(e)}")
                            processing_successful = False
                            break

                    if processing_successful:
                        log_message(f"Successfully processed message from topic '{topic}'")
                    else:
                        log_message(f"Failed to process message from topic '{topic}'")
                else:
                    log_message(f"Unknown topic: {topic}")
            except Exception as e:
                log_message(f"Error processing message: {str(e)}")
                log_message(f"Error details: {type(e).__name__}: {str(e)}")
                continue

    except KeyboardInterrupt:
        log_message("Kafka consumer interrupted.")
    except Exception as e:
        log_message(f"Unexpected error in Kafka consumer: {str(e)}")
        log_message(f"Error details: {type(e).__name__}: {str(e)}")
    finally:
        try:
            consumer.close()
            log_message("Kafka consumer closed.")
        except Exception as e:
            log_message(f"Error closing Kafka consumer: {str(e)}")

def publish_kafka_messages(topic, params=None):
    """
    Publish a message to a Kafka topic.
    
    Args:
        topic (ProducerKafkaTopics): The Kafka topic to publish the message to.
        params (dict, optional): The message payload to be serialized as JSON.
    """
    log_message(f"Publishing Kafka message to topic: {topic.value}...")

    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'acks': 'all',
        'retries': 3,
        'retry.backoff.ms': 1000,
        'request.timeout.ms': 30000,
        'max.block.ms': 60000,
        'enable.idempotence': True,
        'compression.type': 'snappy',
        'linger.ms': 5,
        'batch.size': 16384
    }

    try:
        producer = Producer(producer_config)
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