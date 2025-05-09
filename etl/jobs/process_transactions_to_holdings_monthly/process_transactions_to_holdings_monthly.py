from datetime import datetime
from dateutil.relativedelta import relativedelta
from etl.utils import get_db_connection, log_message
from etl.process_transactions_utils import (
    get_assets_by_transaction_ids,
    remove_all_holdings_for_account_if_no_transactions_exist,
    get_all_accounts_and_assets,
    remove_orphaned_data,
    get_all_assets,
    get_earliest_transaction_date,
)
from main import publish_kafka_messages, ProducerKafkaTopics

def insert_or_update_holdings_monthly(cursor, account_id, asset_name, date, total_balance, unit, symbol, asset_type):
    """
    Insert or update a record in the holdings_monthly table.
    """
    log_message(f"Inserting or updating holdings_monthly for account_id: {account_id}, asset_name: {asset_name}, date: {date}, total_balance: {total_balance}, unit: {unit}, symbol: {symbol}, asset_type: {asset_type}")
    cursor.execute("""
        INSERT INTO holdings_monthly (account_id, asset_name, date, total_balance, unit, symbol, asset_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id, asset_name, date) DO UPDATE
        SET total_balance = EXCLUDED.total_balance,
            unit = EXCLUDED.unit,
            symbol = EXCLUDED.symbol,
            asset_type = EXCLUDED.asset_type
    """, (account_id, asset_name, date, total_balance, unit, symbol, asset_type))

def align_to_first_day_of_month(date):
    """
    Align a given date to the 1st day of its month.
    """
    return date.replace(day=1)

def remove_invalid_monthly_holdings(account_id, assets):
    """
    Remove invalid monthly holdings for assets where deleted transactions invalidate subsequent holdings.
    Log the invalid records before deleting them.
    """
    if not assets:
        log_message(f"No assets to process for account_id: {account_id}")
        return

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Checking for invalid monthly holdings for account_id: {account_id}, assets: {assets}...")

        # Fetch invalid holdings
        cursor.execute("""
            SELECT hm.account_id, hm.asset_name, hm.date, hm.total_balance, hm.unit, hm.symbol, hm.asset_type
            FROM holdings_monthly hm
            WHERE hm.account_id = %s AND hm.asset_name = ANY(%s)
            AND EXISTS (
                SELECT 1
                FROM transactions t
                WHERE t.account_id = hm.account_id
                AND t.asset_name = hm.asset_name
                AND t.deleted_at IS NOT NULL
                AND t.date <= hm.date
            )
        """, (account_id, assets))
        invalid_holdings = cursor.fetchall()

        if invalid_holdings:
            log_message(f"Invalid monthly holdings found for account_id: {account_id}: {invalid_holdings}")

            # Remove invalid holdings
            cursor.execute("""
                DELETE FROM holdings_monthly
                WHERE account_id = %s AND asset_name = ANY(%s)
                AND EXISTS (
                    SELECT 1
                    FROM transactions t
                    WHERE t.account_id = holdings_monthly.account_id
                    AND t.asset_name = holdings_monthly.asset_name
                    AND t.deleted_at IS NOT NULL
                    AND t.date <= holdings_monthly.date
                )
            """, (account_id, assets))
            log_message(f"Removed invalid monthly holdings for account_id: {account_id}, assets: {assets}")
        else:
            log_message(f"No invalid monthly holdings found for account_id: {account_id}, assets: {assets}")

        connection.commit()
    except Exception as e:
        log_message(f"Error while removing invalid monthly holdings: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def calculate_monthly_holdings(account_id, assets, start_date):
    """
    Calculate and update the monthly holdings for each account's asset_name.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for asset_name in assets:
            # Retrieve unit and symbol from the asset table
            cursor.execute("""
                SELECT unit, symbol, asset_type
                FROM asset
                WHERE account_id = %s AND asset_name = %s
            """, (account_id, asset_name))
            result = cursor.fetchone()
            if not result:
                log_message(f"Error: Asset '{asset_name}' not found in the asset table for account_id: {account_id}.")
                continue

            unit, symbol, asset_type = result

            # Align start_date to the 1st day of the month
            aligned_start_date = align_to_first_day_of_month(start_date)
            if start_date != aligned_start_date:
                current_date = aligned_start_date + relativedelta(months=1)
            else:
                current_date = aligned_start_date

            while current_date <= align_to_first_day_of_month(datetime.utcnow().date()):
                # Aggregate transactions up to the current date
                cursor.execute("""
                    SELECT SUM(credit) - SUM(debit) AS total_balance
                    FROM transactions
                    WHERE account_id = %s AND asset_name = %s AND date < %s AND deleted_at IS NULL
                """, (account_id, asset_name, current_date))
                total_balance = cursor.fetchone()[0]

                # Skip updating if no transactions exist for the asset
                if total_balance is None or total_balance == 0:
                    log_message(f"No transactions found for account_id: {account_id}, asset_name: {asset_name}, date: {current_date}. Skipping update.")
                    current_date += relativedelta(months=1)
                    continue

                # Use the new method to insert or update the monthly holdings table
                insert_or_update_holdings_monthly(cursor, account_id, asset_name, current_date, total_balance, unit, symbol, asset_type)

                # Move to the next month
                current_date += relativedelta(months=1)

        connection.commit()
    except Exception as e:
        log_message(f"Error while updating monthly holdings: {e}")
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
    publish_kafka_messages(ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS_MONTHLY_COMPLETE, params)

def run(message_payload=None):
    """
    Main function to calculate and update monthly holdings.
    If message_payload is provided, process for the specific account_id and assets.
    If no message_payload is provided, process for all accounts and their assets.
    """
    log_message("Starting process_transactions_to_holdings_monthly job...")

    if message_payload:
        log_message(f"Received message payload: {message_payload}")

        account_id = message_payload.get("account_id")
        transactions_added = message_payload.get("transactions_added", [])
        transactions_deleted = message_payload.get("transactions_deleted", [])

        # Retrieve assets for added and deleted transactions
        added_assets = get_assets_by_transaction_ids(transactions_added)
        deleted_assets = get_assets_by_transaction_ids(transactions_deleted, True)

        log_message(f"Transactions added: {transactions_added}, Transactions deleted: {transactions_deleted}")
        log_message(f"Processing account_id: {account_id} with added assets: {added_assets}, deleted assets: {deleted_assets}")

        # Check if no transactions exist and remove all holdings if necessary
        remove_all_holdings_for_account_if_no_transactions_exist(account_id)

        # Remove orphaned monthly holdings for deleted assets
        remove_orphaned_data(account_id, deleted_assets, "holdings_monthly")

        # Remove invalid monthly holdings for both deleted and added assets
        remove_invalid_monthly_holdings(account_id, list(set(added_assets + deleted_assets)))

        # Use the 1st day of the month of earliest date of all transactions as the start_date
        start_date = get_earliest_transaction_date(account_id)

        for asset_name in get_all_assets(account_id):
            log_message(f"Processing account_id: {account_id}, asset_name: {asset_name}, start_date: {start_date}")
            calculate_monthly_holdings(account_id, [asset_name], start_date)

    else:
        log_message("No message payload provided. Processing all accounts and assets.")

        # Use the 1st day of the previous month as the start_date
        start_date = align_to_first_day_of_month(datetime.utcnow().date() - relativedelta(months=1))

        # Retrieve all accounts and their assets
        accounts_and_assets = get_all_accounts_and_assets()

        for account_id, asset_name in accounts_and_assets:
            log_message(f"Processing account_id: {account_id}, asset_name: {asset_name}, start_date: {start_date}")
            calculate_monthly_holdings(account_id, [asset_name], start_date)

    # Publish a Kafka message indicating the job is complete
    publish_transactions_processed()

    log_message("process_transactions_to_holdings_monthly job completed successfully.")