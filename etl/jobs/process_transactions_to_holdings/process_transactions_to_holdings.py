import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from main import publish_kafka_messages, ProducerKafkaTopics

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def aggregate_transactions(account_id, symbols):
    """
    Aggregate transactions by account_id and asset_name to calculate total balances.
    Only process transactions for the given account_id and symbols.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT account_id, asset_name, symbol, unit, 
                SUM(credit) - SUM(debit) AS total_balance
            FROM transactions
            WHERE account_id = %s AND symbol = ANY(%s)
            GROUP BY account_id, asset_name, symbol, unit
        """, (account_id, symbols))
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
    Remove any asset_name from holdings that no longer exists in transactions.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Step 1: Update or insert holdings based on transactions
        processed_asset_names = set()
        for transaction in transactions:
            account_id, asset_name, symbol, unit, total_balance = transaction

            log_message(f"Updating holdings for account_id: {account_id}, asset_name: {asset_name}, symbol: {symbol}, total_balance: {total_balance}, unit: {unit}")

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

            # Track processed asset_names for this account
            processed_asset_names.add((account_id, asset_name))

        # Step 2: Remove orphaned records from holdings
        log_message("Removing orphaned records from holdings...")
        cursor.execute("""
            DELETE FROM holdings
            WHERE account_id = %s AND (account_id, asset_name) NOT IN (
                SELECT account_id, asset_name
                FROM transactions
                WHERE account_id = %s
                GROUP BY account_id, asset_name
            )
        """, (account_id, account_id))

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

    # Extract account_id and symbols from the payload
    account_id = message_payload.get("account_id")
    transactions_added = message_payload.get("transactions_added", [])
    symbols = list({transaction["symbol"] for transaction in transactions_added})

    log_message(f"Processing account_id: {account_id} with symbols: {symbols}")

    # Aggregate transactions for the given account_id and symbols
    aggregated_transactions = aggregate_transactions(account_id, symbols)

    log_message(f"Aggregated transactions: {aggregated_transactions}")

    # Update holdings based on the aggregated transactions
    update_holdings(aggregated_transactions)

    # Publish a Kafka message indicating the job is complete
    publish_transactions_processed()
    log_message("process_transactions_to_holdings job completed successfully.")


if __name__ == "__main__":
    run()