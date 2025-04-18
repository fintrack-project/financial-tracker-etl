from datetime import datetime
from dateutil.relativedelta import relativedelta
from etl.utils import get_db_connection, log_message
from main import publish_kafka_messages, ProducerKafkaTopics

def get_assets_by_transaction_ids(transaction_ids):
    """
    Retrieve asset names associated with the given transaction IDs.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT asset_name
            FROM transactions
            WHERE transaction_id = ANY(%s) AND deleted_at IS NULL
        """, (transaction_ids,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()

def get_all_accounts_and_assets():
    """
    Retrieve all account IDs and their associated asset names from the transactions table.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT account_id, asset_name
            FROM transactions
            WHERE deleted_at IS NULL
        """)
        accounts_and_assets = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    return accounts_and_assets

def align_to_first_day_of_month(date):
    """
    Align a given date to the 1st day of its month.
    """
    return date.replace(day=1)

def remove_orphaned_monthly_holdings(account_id, assets):
    """
    Remove orphaned monthly holdings for assets that no longer have any transactions.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for asset_name in assets:
            # Check if the asset still has any transactions
            cursor.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE account_id = %s AND asset_name = %s AND deleted_at IS NULL
            """, (account_id, asset_name))
            transaction_count = cursor.fetchone()[0]

            if transaction_count == 0:
                # Remove orphaned monthly holdings
                cursor.execute("""
                    DELETE FROM monthly_holdings
                    WHERE account_id = %s AND asset_name = %s
                """, (account_id, asset_name))
                log_message(f"Removed orphaned monthly holdings for account_id: {account_id}, asset_name: {asset_name}")

        connection.commit()
    except Exception as e:
        log_message(f"Error while removing orphaned monthly holdings: {e}")
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
                SELECT unit, symbol
                FROM asset
                WHERE account_id = %s AND asset_name = %s
            """, (account_id, asset_name))
            result = cursor.fetchone()
            if not result:
                log_message(f"Error: Asset '{asset_name}' not found in the asset table for account_id: {account_id}.")
                continue

            unit, symbol = result

            current_date = align_to_first_day_of_month(start_date) + relativedelta(months=1)

            while current_date <= align_to_first_day_of_month(datetime.utcnow().date()):
                # Aggregate transactions up to the current date
                cursor.execute("""
                    SELECT SUM(credit) - SUM(debit) AS total_balance
                    FROM transactions
                    WHERE account_id = %s AND asset_name = %s AND date <= %s AND deleted_at IS NULL
                """, (account_id, asset_name, current_date))
                total_balance = cursor.fetchone()[0] or 0

                # Update the monthly holdings table
                cursor.execute("""
                    INSERT INTO holdings_monthly (account_id, asset_name, date, total_balance, unit, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, asset_name, date) DO UPDATE
                    SET total_balance = EXCLUDED.total_balance,
                        unit = EXCLUDED.unit,
                        symbol = EXCLUDED.symbol
                """, (account_id, asset_name, current_date, total_balance, unit, symbol))

                log_message(f"Updated monthly holdings for account_id: {account_id}, asset_name: {asset_name}, date: {current_date}, total_balance: {total_balance}, unit: {unit}, symbol: {symbol}")

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
        deleted_assets = get_assets_by_transaction_ids(transactions_deleted)

        # Determine the start_date as the oldest date from transactions_added or transactions_deleted
        connection = get_db_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT MIN(date)
                FROM transactions
                WHERE transaction_id = ANY(%s) OR transaction_id = ANY(%s)
            """, (transactions_added, transactions_deleted))
            start_date = cursor.fetchone()[0] or datetime.utcnow().date()
        finally:
            cursor.close()
            connection.close()

        log_message(f"Processing account_id: {account_id} with added assets: {added_assets}, deleted assets: {deleted_assets}, start_date: {start_date}")

        # Remove orphaned monthly holdings for deleted assets
        remove_orphaned_monthly_holdings(account_id, deleted_assets)

        # Calculate and update monthly holdings for added assets
        calculate_monthly_holdings(account_id, added_assets, start_date)
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