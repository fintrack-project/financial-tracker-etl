import sys
import os
from datetime import datetime
from confluent_kafka import Producer
from etl.utils import get_db_connection, log_message
from etl.process_transactions_utils import (
    get_assets_by_transaction_ids,
    remove_all_holdings_for_account_if_no_transactions_exist,
    remove_orphaned_data,
    process_affected_assets_optimized,
    process_batch_transactions_optimized,
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

def update_holdings_for_specific_assets(account_id, asset_names):
    """
    Update the holdings table for specific assets only.
    This is much more efficient than updating all assets.
    """
    if not asset_names:
        log_message(f"No specific assets to update for account_id: {account_id}")
        return

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Updating holdings for account_id: {account_id} for specific assets: {asset_names}")

        # OPTIMIZATION: Single query to get all asset data for specific assets
        if len(asset_names) > 1:
            # Use temporary table for multiple assets
            cursor.execute("""
                CREATE TEMP TABLE temp_assets (asset_name TEXT)
                ON COMMIT DROP
            """)
            
            for asset_name in asset_names:
                cursor.execute("""
                    INSERT INTO temp_assets (asset_name) VALUES (%s)
                """, (asset_name,))
            
            cursor.execute("""
                SELECT t.asset_name, t.symbol, t.unit, t.asset_type, 
                       SUM(t.credit) - SUM(t.debit) AS total_balance
                FROM transactions t
                JOIN temp_assets ta ON t.asset_name = ta.asset_name
                WHERE t.account_id = %s AND t.deleted_at IS NULL
                GROUP BY t.asset_name, t.symbol, t.unit, t.asset_type
            """, (account_id,))
            
            results = cursor.fetchall()
            
            for result in results:
                asset_name, symbol, unit, asset_type, total_balance = result
                if total_balance is not None and total_balance != 0:
                    log_message(f"Calculating total balance for asset_name: {asset_name}, total_balance: {total_balance}, symbol: {symbol}, unit: {unit}, asset_type: {asset_type}")
                    insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance, asset_type)
                else:
                    log_message(f"No balance for asset_name: {asset_name}, removing from holdings")
                    cursor.execute("""
                        DELETE FROM holdings
                        WHERE account_id = %s AND asset_name = %s
                    """, (account_id, asset_name))
        else:
            # Original logic for single asset (maintains existing behavior)
            asset_name = asset_names[0]
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
                insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance, asset_type)
            else:
                log_message(f"No transactions found for asset_name: {asset_name}, removing from holdings")
                cursor.execute("""
                    DELETE FROM holdings
                    WHERE account_id = %s AND asset_name = %s
                """, (account_id, asset_name))

        # Commit the changes to the database
        connection.commit()
        log_message(f"Holdings table updated successfully for account_id: {account_id} for assets: {asset_names}")

    except Exception as e:
        log_message(f"Error while updating holdings for account_id {account_id}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def update_all_holdings(account_id):
    """
    Update the holdings table with the total balance for each account's asset_name.
    This is the original function kept for backward compatibility.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Fetching all asset names for account_id: {account_id} from non-deleted transactions...")

        # OPTIMIZATION: Single query to get all asset data
        cursor.execute("""
            SELECT asset_name, symbol, unit, asset_type, 
                   SUM(credit) - SUM(debit) AS total_balance
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
            GROUP BY asset_name, symbol, unit, asset_type
        """, (account_id,))
        
        results = cursor.fetchall()

        if not results:
            log_message(f"No assets found for account_id: {account_id}.")
            return

        log_message(f"Found {len(results)} assets for account_id {account_id}")

        # Process all results in a single batch
        for result in results:
            asset_name, symbol, unit, asset_type, total_balance = result
            
            if total_balance is not None and total_balance != 0:
                log_message(f"Calculating total balance for asset_name: {asset_name}, total_balance: {total_balance}, symbol: {symbol}, unit: {unit}, asset_type: {asset_type}")
                insert_or_update_holding(cursor, account_id, asset_name, symbol, unit, total_balance, asset_type)
            else:
                log_message(f"No balance for asset_name: {asset_name}, removing from holdings")
                cursor.execute("""
                    DELETE FROM holdings
                    WHERE account_id = %s AND asset_name = %s
                """, (account_id, asset_name))

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
    Uses the common utility function for optimized processing.
    """
    log_message("Starting process_transactions_to_holdings job...")
    log_message(f"Received message payload: {message_payload}")

    # Use the common utility function for optimized processing
    result = process_batch_transactions_optimized(
        message_payload=message_payload,
        holdings_processor_func=update_holdings_for_specific_assets,
        monthly_processor_func=None,  # No monthly processing for regular holdings
        start_date=None
    )
    
    # Publish completion message
    publish_kafka_messages(
        ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS_COMPLETE,
        result
    )
    
    log_message(f"process_transactions_to_holdings job completed successfully in {result['processingTimeMs']}ms.")


if __name__ == "__main__":
    run()