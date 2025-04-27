import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from etl.process_transactions_utils import (
    get_assets_by_transaction_ids,
    remove_all_holdings_for_account_if_no_transactions_exist,
    remove_orphaned_data,
)
from main import publish_kafka_messages, ProducerKafkaTopics

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance, asset_type):
    """
    Insert or update a holding record in the holdings table.
    """
    log_message(f"Updating holdings for account_id: {account_id}, asset_name: {asset_name}, symbol: {symbol}, total_balance: {total_balance}, unit: {unit}, asset_type: {asset_type}")
    cursor.execute("""
        INSERT INTO holdings (account_id, asset_name, symbol, total_balance, unit, asset_type, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id, asset_name) DO UPDATE
        SET total_balance = EXCLUDED.total_balance,
            unit = EXCLUDED.unit,
            symbol = EXCLUDED.symbol,
            asset_type = EXCLUDED.asset_type,
            updated_at = EXCLUDED.updated_at
    """, (account_id, asset_name, symbol, total_balance, unit, asset_type))

def update_holdings(account_id):
    """
    Update the holdings table with the total balance for each account's asset_name.
    Retrieve all asset_name values for the given account_id from non-deleted transactions
    and calculate the total balance for each asset_name.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Fetching all asset names for account_id: {account_id} from non-deleted transactions...")

        # Step 1: Get all asset_name values for the account_id from non-deleted transactions
        cursor.execute("""
            SELECT DISTINCT asset_name
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
        """, (account_id,))
        asset_names = [row[0] for row in cursor.fetchall()]

        if not asset_names:
            log_message(f"No assets found for account_id: {account_id}.")
            return

        log_message(f"Found assets for account_id {account_id}: {asset_names}")

        # Step 2: Calculate the total balance for each asset_name
        for asset_name in asset_names:
            cursor.execute("""
                SELECT SUM(credit) - SUM(debit) AS total_balance, symbol, unit, asset_type
                FROM transactions
                WHERE account_id = %s AND asset_name = %s AND deleted_at IS NULL
                GROUP BY symbol, unit, asset_type
            """, (account_id, asset_name))
            result = cursor.fetchone()

            if result:
                total_balance, symbol, unit, asset_type = result
                log_message(f"Calculating total balance for asset_name: {asset_name}, total_balance: {total_balance}, symbol: {symbol}, unit: {unit}, asset_type: {asset_type}")

                # Step 3: Insert or update the holdings table
                insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance, asset_type)

        # Commit the changes to the database
        connection.commit()
        log_message(f"Holdings table updated successfully for account_id: {account_id}.")

    except Exception as e:
        log_message(f"Error while updating holdings for account_id {account_id}: {e}")
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

    # Check if no transactions exist and remove all holdings if necessary
    remove_all_holdings_for_account_if_no_transactions_exist(account_id)

    # Retrieve assets for added and deleted transactions
    added_assets = get_assets_by_transaction_ids(transactions_added)
    deleted_assets = get_assets_by_transaction_ids(transactions_deleted, True)

    log_message(f"Transactions added: {transactions_added}, Transactions deleted: {transactions_deleted}")
    log_message(f"Processing account_id: {account_id} with added assets: {added_assets}, deleted assets: {deleted_assets}")

    # Step 4: Remove orphaned records from holdings
    remove_orphaned_data(account_id, deleted_assets, "holdings")

    # Update holdings of account
    update_holdings(account_id)

    # Publish a Kafka message indicating the job is complete
    publish_transactions_processed()
    log_message("process_transactions_to_holdings job completed successfully.")


if __name__ == "__main__":
    run()