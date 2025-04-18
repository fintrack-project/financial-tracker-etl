import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from main import publish_kafka_messages, ProducerKafkaTopics

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def aggregate_transactions(account_id, transaction_ids):
    """
    Aggregate transactions by account_id and asset_name to calculate total balances.
    Only process transactions for the given account_id and transaction_ids.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT account_id, asset_name, symbol, unit, 
                SUM(credit) - SUM(debit) AS total_balance
            FROM transactions
            WHERE account_id = %s AND transaction_id = ANY(%s) AND deleted_at IS NULL
            GROUP BY account_id, asset_name, symbol, unit
        """, (account_id, transaction_ids))
        transactions = cursor.fetchall()
        return transactions
    except Exception as e:
        log_message(f"Error while aggregating transactions: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance):
    """
    Insert or update a holding record in the holdings table.
    """
    log_message(f"Updating holdings for account_id: {account_id}, asset_name: {asset_name}, symbol: {symbol}, total_balance: {total_balance}, unit: {unit}")
    cursor.execute("""
        INSERT INTO holdings (account_id, asset_name, symbol, total_balance, unit, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id, asset_name) DO UPDATE
        SET total_balance = EXCLUDED.total_balance,
            unit = EXCLUDED.unit,
            symbol = EXCLUDED.symbol,
            updated_at = EXCLUDED.updated_at
    """, (account_id, asset_name, symbol, total_balance, unit))


def remove_orphaned_holdings(cursor, account_id):
    """
    Remove orphaned records from the holdings table for the given account_id.
    """
    log_message("Removing orphaned records from holdings...")
    cursor.execute("""
        DELETE FROM holdings
        WHERE account_id = %s AND (account_id, asset_name) NOT IN (
            SELECT account_id, asset_name
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
            GROUP BY account_id, asset_name
        )
    """, (account_id, account_id))
    log_message("Orphaned records removed successfully.")

def update_holdings(account_id, aggregated_transactions):
    """
    Update the holdings table with the total balance for each account's asset_name.
    Remove any asset_name from holdings that no longer exists in transactions.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Step 1: Update or insert holdings based on aggregated transactions
        for transaction in aggregated_transactions:
            account_id, asset_name, symbol, unit, total_balance = transaction
            insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance)

        # Step 2: Remove orphaned records from holdings
        remove_orphaned_holdings(cursor, account_id)

        # Commit the changes to the database
        connection.commit()
        log_message("Holdings table updated successfully, including removal of orphaned records.")

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
    publish_kafka_messages(ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS_COMPLETE, params)

def run(message_payload):
    """
    Main function to calculate and update holdings based on the Kafka message payload.
    """
    log_message("Starting process_transactions_to_holdings job...")
    log_message(f"Received message payload: {message_payload}")

    # Extract account_id and transaction IDs from the payload
    account_id = message_payload.get("account_id")
    transactions_added = message_payload.get("transactions_added", [])
    transactions_deleted = message_payload.get("transactions_deleted", [])

    log_message(f"Processing account_id: {account_id} with added transactions: {transactions_added} and deleted transactions: {transactions_deleted}")

    # Aggregate transactions for the added transaction IDs
    aggregated_transactions = aggregate_transactions(account_id, transactions_added)

    log_message(f"Aggregated transactions: {aggregated_transactions}")

    # Update holdings based on the aggregated transactions
    update_holdings(account_id, aggregated_transactions)

    # Publish a Kafka message indicating the job is complete
    publish_transactions_processed()
    log_message("process_transactions_to_holdings job completed successfully.")


if __name__ == "__main__":
    run()