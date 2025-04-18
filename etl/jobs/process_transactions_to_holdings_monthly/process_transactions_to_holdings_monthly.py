from datetime import datetime
from dateutil.relativedelta import relativedelta
from etl.utils import get_db_connection, log_message

def get_oldest_transaction_date(account_id, symbol):
    """
    Get the oldest transaction date for a given account_id and symbol.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT MIN(date)
            FROM transactions
            WHERE account_id = %s AND symbol = %s
        """, (account_id, symbol))
        return cursor.fetchone()[0]
    finally:
        cursor.close()
        connection.close()

def get_most_recent_monthly_date(account_id, symbol, oldest_transaction_date):
    """
    Get the most recent monthly holdings date older than the oldest transaction date.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT MAX(date)
            FROM monthly_holdings
            WHERE account_id = %s AND symbol = %s AND date < %s
        """, (account_id, symbol, oldest_transaction_date))
        return cursor.fetchone()[0]
    finally:
        cursor.close()
        connection.close()

def align_to_first_day_of_month(date):
    """
    Align a given date to the 1st day of its month.
    """
    return date.replace(day=1)

def update_monthly_holdings(account_id, symbol, start_date):
    """
    Update the monthly holdings for a given account_id and symbol starting from the 1st day of the month.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Align the start_date to the 1st day of the next month
        current_date = align_to_first_day_of_month(start_date) + relativedelta(months=1)

        while current_date <= align_to_first_day_of_month(datetime.utcnow().date()):
            # Aggregate transactions up to the current date
            cursor.execute("""
                SELECT SUM(credit) - SUM(debit) AS total_balance
                FROM transactions
                WHERE account_id = %s AND symbol = %s AND date <= %s
            """, (account_id, symbol, current_date))
            total_balance = cursor.fetchone()[0] or 0

            # Update the monthly holdings table
            cursor.execute("""
                INSERT INTO monthly_holdings (account_id, symbol, date, total_balance)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (account_id, symbol, date) DO UPDATE
                SET total_balance = EXCLUDED.total_balance
            """, (account_id, symbol, current_date, total_balance))

            log_message(f"Updated monthly holdings for account_id: {account_id}, symbol: {symbol}, date: {current_date}, total_balance: {total_balance}")

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

def remove_orphaned_monthly_holdings(account_id, symbols):
    """
    Remove orphaned monthly holdings for symbols that no longer have any transactions.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for symbol in symbols:
            # Check if the symbol still has any transactions
            cursor.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE account_id = %s AND symbol = %s
            """, (account_id, symbol))
            transaction_count = cursor.fetchone()[0]

            if transaction_count == 0:
                # Remove orphaned monthly holdings
                cursor.execute("""
                    DELETE FROM monthly_holdings
                    WHERE account_id = %s AND symbol = %s
                """, (account_id, symbol))
                log_message(f"Removed orphaned monthly holdings for account_id: {account_id}, symbol: {symbol}")

        connection.commit()
    except Exception as e:
        log_message(f"Error while removing orphaned monthly holdings: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def calculate_monthly_holdings(account_id, symbols):
    """
    Calculate and update the monthly holdings for each account's asset_name.
    """
    for symbol in symbols:
        oldest_transaction_date = get_oldest_transaction_date(account_id, symbol)
        if not oldest_transaction_date:
            log_message(f"No transactions found for account_id: {account_id}, symbol: {symbol}. Skipping.")
            continue

        most_recent_monthly_date = get_most_recent_monthly_date(account_id, symbol, oldest_transaction_date)
        if not most_recent_monthly_date:
            # If no monthly holdings exist, start from the oldest transaction date
            most_recent_monthly_date = oldest_transaction_date - relativedelta(months=1)

        update_monthly_holdings(account_id, symbol, most_recent_monthly_date)

def run(message_payload):
    """
    Main function to calculate and update monthly holdings based on the Kafka message payload.
    """
    log_message("Starting process_transactions_to_holdings_monthly job...")
    log_message(f"Received message payload: {message_payload}")

    account_id = message_payload.get("account_id")
    transactions_added = message_payload.get("transactions_added", [])
    transactions_deleted = message_payload.get("transactions_deleted", [])

    # Extract symbols from added and deleted transactions
    added_symbols = list({transaction["symbol"] for transaction in transactions_added})
    deleted_symbols = list({transaction["symbol"] for transaction in transactions_deleted})

    log_message(f"Processing account_id: {account_id} with added symbols: {added_symbols} and deleted symbols: {deleted_symbols}")

    # Remove orphaned monthly holdings for deleted symbols
    remove_orphaned_monthly_holdings(account_id, deleted_symbols)

    # Calculate and update monthly holdings for added symbols
    calculate_monthly_holdings(account_id, added_symbols)

    log_message("process_transactions_to_holdings_monthly job completed successfully.")