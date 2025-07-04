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

def process_affected_assets_optimized(account_id, transactions_added, transactions_deleted, 
                                    holdings_processor_func, monthly_processor_func=None, 
                                    start_date=None, table_name="holdings"):
    """
    Common utility function to process only affected assets for both holdings and monthly holdings.
    This eliminates code duplication and ensures consistent processing logic.
    
    Parameters:
        account_id (str): The account ID to process
        transactions_added (list): List of transaction IDs that were added
        transactions_deleted (list): List of transaction IDs that were deleted
        holdings_processor_func (function): Function to process holdings updates
        monthly_processor_func (function, optional): Function to process monthly holdings updates
        start_date (date, optional): Start date for monthly calculations
        table_name (str): Table name for orphaned data removal (default: "holdings")
    
    Returns:
        tuple: (affected_assets, processing_time_ms)
    """
    import time
    from datetime import datetime
    
    start_time = time.time()
    
    log_message(f"Processing affected assets for account_id: {account_id}")
    log_message(f"Transactions added: {transactions_added}, Transactions deleted: {transactions_deleted}")
    
    # Check if no transactions exist and remove all holdings if necessary
    remove_all_holdings_for_account_if_no_transactions_exist(account_id)
    
    # Retrieve assets for added and deleted transactions
    added_assets = get_assets_by_transaction_ids(transactions_added)
    deleted_assets = get_assets_by_transaction_ids(transactions_deleted, True)
    
    log_message(f"Added assets: {added_assets}, Deleted assets: {deleted_assets}")
    
    # Remove orphaned records from holdings
    remove_orphaned_data(account_id, deleted_assets, table_name)
    
    # Combine added and deleted assets to get all affected assets
    affected_assets = list(set(added_assets + deleted_assets))
    
    if affected_assets:
        log_message(f"Processing {len(affected_assets)} affected assets: {affected_assets}")
        
        # Process holdings updates using optimized function
        if holdings_processor_func:
            holdings_processor_func(account_id, affected_assets)

        # Process monthly holdings updates if function provided
        if monthly_processor_func and start_date:
            log_message(f"Processing monthly holdings for {len(affected_assets)} affected assets")
            monthly_processor_func(account_id, affected_assets, start_date)
    else:
        log_message("No affected assets to process")

    processing_time_ms = int((time.time() - start_time) * 1000)
    log_message(f"Processed {len(affected_assets)} affected assets in {processing_time_ms}ms")

    return affected_assets, processing_time_ms

def process_batch_transactions_optimized(message_payload, holdings_processor_func, 
                                       monthly_processor_func=None, start_date=None):
    """
    Common utility function to process batch transactions efficiently.
    Handles both single account and multiple account scenarios.
    
    Parameters:
        message_payload (dict): The Kafka message payload
        holdings_processor_func (function): Function to process holdings updates
        monthly_processor_func (function, optional): Function to process monthly holdings updates
        start_date (date, optional): Start date for monthly calculations
    
    Returns:
        dict: Processing results with metadata
    """
    import time
    from datetime import datetime
    
    start_time = time.time()
    
    # Handle different payload formats
    if isinstance(message_payload, dict) and "accounts" in message_payload:
        # Batch format with multiple accounts
        accounts_data = message_payload["accounts"]
        total_transactions = 0
        all_affected_assets = []
        
        for account_data in accounts_data:
            account_id = account_data.get("account_id")
            transactions_added = account_data.get("transactions_added", [])
            transactions_deleted = account_data.get("transactions_deleted", [])
            
            total_transactions += len(transactions_added)
            
            log_message(f"Processing account_id: {account_id} with {len(transactions_added)} added and {len(transactions_deleted)} deleted transactions")
            
            # Use the common processing function
            affected_assets, _ = process_affected_assets_optimized(
                account_id, transactions_added, transactions_deleted,
                holdings_processor_func, monthly_processor_func, start_date
            )
            all_affected_assets.extend(affected_assets)
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        return {
            "accounts": [data.get("account_id") for data in accounts_data],
            "assets": list(set(all_affected_assets)),  # Remove duplicates
            "totalBatches": 1,
            "totalTransactions": total_transactions,
            "processingTimeMs": processing_time_ms,
            "status": "complete"
        }
        
    else:
        # Single account format
        account_id = message_payload.get("account_id")
        transactions_added = message_payload.get("transactions_added", [])
        transactions_deleted = message_payload.get("transactions_deleted", [])
        
        # Use the common processing function
        affected_assets, processing_time_ms = process_affected_assets_optimized(
            account_id, transactions_added, transactions_deleted,
            holdings_processor_func, monthly_processor_func, start_date
        )
        
        return {
            "account_id": account_id,
            "assets": affected_assets,
            "totalBatches": 1,
            "totalTransactions": len(transactions_added),
            "processingTimeMs": processing_time_ms,
            "status": "complete"
        }
