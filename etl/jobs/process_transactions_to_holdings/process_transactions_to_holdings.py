import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from main import publish_kafka_messages, ProducerKafkaTopics

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def aggregate_transactions():
    """
    Aggregate transactions by account_id and asset_name to calculate total balances.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT account_id, asset_name, symbol, unit, 
                SUM(credit) - SUM(debit) AS total_balance
            FROM transactions
            GROUP BY account_id, asset_name, symbol, unit
        """)
        transactions = cursor.fetchall()
        return transactions
    except Exception as e:
        log_message(f"Error while aggregating transactions: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def update_holdings(transactions):
    """
    Update the holdings table with the total balance for each account's asset_name.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for transaction in transactions:
            account_id, asset_name, symbol, unit, total_balance = transaction

            # Update the holdings table
            cursor.execute("""
                INSERT INTO holdings (account_id, asset_name, symbol, total_balance, unit, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (account_id, asset_name) DO UPDATE
                SET total_balance = EXCLUDED.total_balance,
                    unit = EXCLUDED.unit,
                    symbol = EXCLUDED.symbol,
                    updated_at = EXCLUDED.updated_at
            """, (account_id, asset_name, symbol, total_balance, unit))

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
    Publish a Kafka topic indicating that transactions have been processed.
    """
    # Use the centralized publish_kafka_messages method
    params = {"status": "transactions_processed"}
    publish_kafka_messages(ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS, params)

def run():
    """
    Main function to calculate and update holdings.
    """
    log_message("Starting process_transactions_to_holdings job...")
    aggregated_transactions = aggregate_transactions()
    update_holdings(aggregated_transactions)
    publish_transactions_processed()
    log_message("process_transactions_to_holdings job completed successfully.")

if __name__ == "__main__":
    run()