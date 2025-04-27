from etl.utils import get_db_connection, log_message

def get_all_assets(account_id):
    """
    Retrieve all asset names for a given account_id.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT asset_name
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
        """, (account_id,))
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
        return cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

def get_assets_by_transaction_ids(transaction_ids, deleted=False):
    """
    Retrieve asset names associated with the given transaction IDs.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        if deleted:
            cursor.execute("""
                SELECT DISTINCT asset_name
                FROM transactions
                WHERE transaction_id = ANY(%s) AND deleted_at IS NOT NULL
            """, (transaction_ids,))
        else:
            cursor.execute("""
                SELECT DISTINCT asset_name
                FROM transactions
                WHERE transaction_id = ANY(%s) AND deleted_at IS NULL
            """, (transaction_ids,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()

def get_earliest_transaction_date(account_id):
    """
    Retrieve the earliest transaction date for a given account_id.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT MIN(date)
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
        """, (account_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()
        connection.close()

def remove_all_holdings_for_account_if_no_transactions_exist(account_id):
    """
    Check if no transactions exist for the given account_id.
    If no transactions exist, remove all holdings and monthly holdings for that account.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Checking if any transactions exist for account_id: {account_id}...")

        # Check if there are any non-deleted transactions for the account
        cursor.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE account_id = %s AND deleted_at IS NULL
        """, (account_id,))
        transaction_count = cursor.fetchone()[0]

        if transaction_count == 0:
            log_message(f"No transactions found for account_id: {account_id}. Removing all holdings...")

            # Remove all records from the holdings table
            cursor.execute("""
                DELETE FROM holdings
                WHERE account_id = %s
            """, (account_id,))
            log_message(f"All holdings removed for account_id: {account_id}.")

            # Remove all records from the holdings_monthly table
            cursor.execute("""
                DELETE FROM holdings_monthly
                WHERE account_id = %s
            """, (account_id,))
            log_message(f"All monthly holdings removed for account_id: {account_id}.")

            connection.commit()
        else:
            log_message(f"Transactions exist for account_id: {account_id}. No holdings were removed.")

    except Exception as e:
        log_message(f"Error while checking and removing holdings for account_id {account_id}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def remove_orphaned_data(account_id, assets, table_name, asset_column="asset_name"):
    """
    Remove orphaned records from the specified table for the given account_id and assets.
    Orphaned records are those that no longer have any associated transactions.
    
    Parameters:
        account_id (str): The account ID to process.
        assets (list): List of asset names to check for orphaned records.
        table_name (str): The name of the table to remove orphaned records from.
        asset_column (str): The column name for the asset in the table (default is "asset_name").
    """
    if not assets:
        log_message(f"No assets to process for account_id: {account_id} in table {table_name}.")
        return

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Checking for orphaned records in table {table_name} for account_id: {account_id}, assets: {assets}...")

        # Fetch orphaned records
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE account_id = %s AND {asset_column} = ANY(%s)
            AND NOT EXISTS (
                SELECT 1
                FROM transactions
                WHERE account_id = %s AND {asset_column} = {table_name}.{asset_column} AND deleted_at IS NULL
            )
        """
        cursor.execute(query, (account_id, assets, account_id))
        orphaned_records = cursor.fetchall()

        if orphaned_records:
            log_message(f"Orphaned records found in table {table_name} for account_id: {account_id}, assets: {assets}: {orphaned_records}")

            # Delete orphaned records
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE account_id = %s AND {asset_column} = ANY(%s)
                AND NOT EXISTS (
                    SELECT 1
                    FROM transactions
                    WHERE account_id = %s AND {asset_column} = {table_name}.{asset_column} AND deleted_at IS NULL
                )
            """
            cursor.execute(delete_query, (account_id, assets, account_id))
            log_message(f"Removed orphaned records from table {table_name} for account_id: {account_id}, assets: {assets}.")
        else:
            log_message(f"No orphaned records found in table {table_name} for account_id: {account_id}, assets: {assets}.")

        connection.commit()
    except Exception as e:
        log_message(f"Error while removing orphaned records from table {table_name} for account_id {account_id}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
