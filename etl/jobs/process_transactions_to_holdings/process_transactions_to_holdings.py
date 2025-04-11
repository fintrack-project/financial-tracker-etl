import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from main import ProducerKafkaTopics
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
def fetch_transactions():
    """
    Fetch all transactions ordered by account_id, asset_name, and date.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT account_id, date, asset_name, credit, debit, unit
            FROM transactions
            ORDER BY account_id, asset_name, date
        """)
        transactions = cursor.fetchall()
        return transactions
    except Exception as e:
        log_message(f"Error while fetching transactions: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def update_holdings(transactions):
    """
    Calculate total holdings by asset (asset_name) and update the holdings table.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Initialize a dictionary to track holdings and the most recent date
        holdings = {}
        most_recent_dates = {}

        # Process transactions to calculate holdings
        for transaction in transactions:
            account_id, date, asset_name, credit, debit, unit = transaction

            # Initialize holdings for this account_id and asset_name if not already present
            if (account_id, asset_name) not in most_recent_dates:
                most_recent_dates[(account_id, asset_name)] = date
                holdings[(account_id, date, asset_name)] = 0  # Start with 0 balance

            # Calculate the net change for this transaction
            net_change = (credit or 0) - (debit or 0)

            # Update the holdings balance
            most_recent_date = most_recent_dates[(account_id, asset_name)]
            holdings[(account_id, date, asset_name)] = holdings[(account_id, most_recent_date, asset_name)] + net_change

            if date > most_recent_dates[(account_id, asset_name)]:
                most_recent_dates[(account_id, asset_name)] = date

            # Update the holdings table for this transaction date
            cursor.execute("""
                INSERT INTO holdings (account_id, date, asset_name, total_balance, unit)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_id, date, asset_name) DO UPDATE
                SET total_balance = EXCLUDED.total_balance,
                    unit = EXCLUDED.unit
            """, (account_id, date, asset_name, holdings[(account_id, date, asset_name)], unit))

        # Commit the changes to the database
        connection.commit()
        log_message("Holdings table updated successfully.")

    except Exception as e:
        log_message(f"Error while updating holdings: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def publish_transactions_processed():
    """
    Send a PROCESS_TRANSACTIONS_TO_HOLDINGS message to notify the HoldingService.
    """
    log_message(f"Publishing Kafka topic: {ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS.value}...")
    producer_config = {
        'bootstrap.servers': 'kafka:9093',  # Replace with your Kafka broker address
    }
    producer = Producer(producer_config)

    try:
        message = json.dumps({"status": "transactions_processed"})
        producer.produce(ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS.value, key="transactions", value=message)
        producer.flush()
        log_message(f"Published Kafka topic: {ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS.value}")
    except Exception as e:
        log_message(f"Error publishing Kafka topic: {e}")
        raise

def run():
    """
    Main function to calculate and update holdings.
    """
    log_message("Starting process_transactions_to_holdings job...")
    transactions = fetch_transactions()
    update_holdings(transactions)
    publish_transactions_processed()
    log_message("process_transactions_to_holdings job completed successfully.")

if __name__ == "__main__":
    run()