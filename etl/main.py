import importlib
import sys
import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def process_message_batch(messages, consumer):
    """
    Process a batch of messages in parallel.
    
    Args:
        messages (list): List of Kafka messages to process
        consumer: Kafka consumer instance for committing offsets
    """
    if not messages:
        return
    
    log_message(f"Processing batch of {len(messages)} messages in parallel")
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=min(len(messages), 4)) as executor:
        # Submit all messages for processing
        future_to_message = {}
        for msg in messages:
            future = executor.submit(process_single_message, msg)
            future_to_message[future] = msg
        
        # Collect results and handle any exceptions
        successful_messages = []
        failed_messages = []
        
        for future in as_completed(future_to_message):
            msg = future_to_message[future]
            try:
                result = future.result()
                if result:
                    successful_messages.append(msg)
                else:
                    failed_messages.append(msg)
            except Exception as e:
                log_message(f"Error processing message: {e}")
                failed_messages.append(msg)
        
        # Commit offsets for successful messages
        if successful_messages:
            try:
                # Commit offsets for the last message in each partition
                partitions_to_commit = {}
                for msg in successful_messages:
                    topic_partition = (msg.topic(), msg.partition())
                    if topic_partition not in partitions_to_commit or msg.offset() > partitions_to_commit[topic_partition].offset():
                        partitions_to_commit[topic_partition] = msg
                
                for msg in partitions_to_commit.values():
                    consumer.commit(msg)
                
                log_message(f"Committed offsets for {len(partitions_to_commit)} partitions")
            except Exception as e:
                log_message(f"Error committing offsets: {e}")
        
        log_message(f"Batch processing complete: {len(successful_messages)} successful, {len(failed_messages)} failed")

def process_single_message(msg):
    """
    Process a single Kafka message.
    
    Args:
        msg: Kafka message to process
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log_message(f"Reached end of partition for topic {msg.topic()}, partition {msg.partition()}")
                return True
            else:
                log_message(f"Kafka error: {msg.error()}")
                return False

        # Process the message
        topic = msg.topic()
        value = msg.value().decode('utf-8')
        log_message(f"Processing message on topic '{topic}': {value[:100]}...")  # Truncate for logging
        log_message(f"Message metadata - Partition: {msg.partition()}, Offset: {msg.offset()}, Timestamp: {msg.timestamp()}")

        # Handle empty content
        if not value.strip():
            log_message(f"Warning: Received empty message on topic '{topic}'. Skipping processing.")
            return False

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
                        log_message(f"Successfully parsed JSON payload for job {job_name}")
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
                return True
            else:
                log_message(f"Failed to process message from topic '{topic}'")
                return False
        else:
            log_message(f"Unknown topic: {topic}")
            return False
            
    except Exception as e:
        log_message(f"Error processing message: {str(e)}")
        log_message(f"Error details: {type(e).__name__}: {str(e)}")
        return False

def consume_kafka_messages():
    """
    Consume messages from Kafka and trigger the appropriate job.
    Now with batch processing and parallel execution.
    """
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'etl-job-consumer-group',
        'client.id': f'etl-consumer-{os.getpid()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Disable auto-commit for manual control
        'auto.commit.interval.ms': 5000,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 60000,
        'heartbeat.interval.ms': 20000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 500,
        'max.partition.fetch.bytes': 1048576,
        'check.crcs': True,
        'retries': 3,
        'retry.backoff.ms': 1000
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

    # Batch processing configuration
    batch_size = 10  # Process up to 10 messages per batch
    batch_timeout = 0.5  # Maximum time to wait for batch completion
    message_batch = []

    try:
        while True:
            log_message("Polling for messages...")
            msg = consumer.poll(0.1)  # Poll for messages with a 0.1-second timeout (was 1.0)

            if msg is None:
                # No message received, process any accumulated batch
                if message_batch:
                    log_message(f"No new messages, processing accumulated batch of {len(message_batch)} messages")
                    process_message_batch(message_batch, consumer)
                    message_batch = []
                continue

            # Add message to batch
            message_batch.append(msg)
            
            # Process batch if it reaches the size limit
            if len(message_batch) >= batch_size:
                log_message(f"Batch size reached ({batch_size}), processing batch")
                process_message_batch(message_batch, consumer)
                message_batch = []

    except KeyboardInterrupt:
        log_message("Kafka consumer interrupted.")
        # Process any remaining messages in the batch
        if message_batch:
            log_message(f"Processing final batch of {len(message_batch)} messages before shutdown")
            process_message_batch(message_batch, consumer)
    except Exception as e:
        log_message(f"Unexpected error in Kafka consumer: {str(e)}")
        log_message(f"Error details: {type(e).__name__}: {str(e)}")
        # Process any remaining messages in the batch
        if message_batch:
            log_message(f"Processing final batch of {len(message_batch)} messages before error shutdown")
            process_message_batch(message_batch, consumer)
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