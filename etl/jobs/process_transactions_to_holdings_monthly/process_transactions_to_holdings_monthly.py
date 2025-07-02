from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
from etl.utils import get_db_connection, log_message
from etl.process_transactions_utils import (
    get_assets_by_transaction_ids,
    remove_all_holdings_for_account_if_no_transactions_exist,
    get_all_accounts_and_assets,
    remove_orphaned_data,
    get_all_assets,
    get_earliest_transaction_date,
    process_affected_assets_optimized,
    process_batch_transactions_optimized,
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

def calculate_monthly_holdings_for_specific_assets(account_id, assets, start_date):
    """
    Calculate and update the monthly holdings for specific assets only.
    This is much more efficient than calculating for all assets.
    """
    if not assets:
        log_message(f"No specific assets to calculate monthly holdings for account_id: {account_id}")
        return

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Calculating monthly holdings for account_id: {account_id} for specific assets: {assets}")

        # OPTIMIZATION: Single query to get all asset data for specific assets
        if len(assets) > 1:
            # Use temporary table for multiple assets
            cursor.execute("""
                CREATE TEMP TABLE temp_assets (asset_name TEXT)
                ON COMMIT DROP
            """)
            
            for asset_name in assets:
                cursor.execute("""
                    INSERT INTO temp_assets (asset_name) VALUES (%s)
                """, (asset_name,))
            
            cursor.execute("""
                SELECT DISTINCT t.asset_name, t.symbol, t.unit, t.asset_type
                FROM transactions t
                JOIN temp_assets ta ON t.asset_name = ta.asset_name
                WHERE t.account_id = %s AND t.deleted_at IS NULL
            """, (account_id,))
            
            asset_data = cursor.fetchall()
        else:
            # Original logic for single asset (maintains existing behavior)
            asset_name = assets[0]
            cursor.execute("""
                SELECT unit, symbol, asset_type
                FROM asset
                WHERE account_id = %s AND asset_name = %s
            """, (account_id, asset_name))
            result = cursor.fetchone()
            if not result:
                log_message(f"Error: Asset '{asset_name}' not found in the asset table for account_id: {account_id}.")
                return
            unit, symbol, asset_type = result
            asset_data = [(asset_name, symbol, unit, asset_type)]

        if not asset_data:
            log_message(f"No assets found for account_id: {account_id}")
            return

        log_message(f"Processing {len(asset_data)} assets for monthly holdings for account_id: {account_id}")

        # Align start_date to the 1st day of the month
        aligned_start_date = align_to_first_day_of_month(start_date)
        if start_date != aligned_start_date:
            current_date = aligned_start_date + relativedelta(months=1)
        else:
            current_date = aligned_start_date

        end_date = align_to_first_day_of_month(datetime.utcnow().date())

        # OPTIMIZATION: Single query to get all monthly balances for all assets
        if len(asset_data) > 1:
            # Create temporary table for asset names
            cursor.execute("""
                CREATE TEMP TABLE temp_asset_names (asset_name TEXT)
                ON COMMIT DROP
            """)
            
            for asset_name, _, _, _ in asset_data:
                cursor.execute("""
                    INSERT INTO temp_asset_names (asset_name) VALUES (%s)
                """, (asset_name,))
            
            # Generate all monthly dates
            monthly_dates = []
            monthly_date = current_date
            while monthly_date <= end_date:
                monthly_dates.append(monthly_date)
                monthly_date += relativedelta(months=1)
            
            # Create temporary table for monthly dates
            cursor.execute("""
                CREATE TEMP TABLE temp_monthly_dates (monthly_date DATE)
                ON COMMIT DROP
            """)
            
            for monthly_date in monthly_dates:
                cursor.execute("""
                    INSERT INTO temp_monthly_dates (monthly_date) VALUES (%s)
                """, (monthly_date,))
            
            # Single query to get all monthly balances for all assets
            cursor.execute("""
                SELECT 
                    t.asset_name,
                    md.monthly_date,
                    SUM(t.credit) - SUM(t.debit) AS total_balance
                FROM transactions t
                JOIN temp_asset_names tan ON t.asset_name = tan.asset_name
                CROSS JOIN temp_monthly_dates md
                WHERE t.account_id = %s 
                AND t.deleted_at IS NULL
                AND t.date < md.monthly_date
                GROUP BY t.asset_name, md.monthly_date
                ORDER BY t.asset_name, md.monthly_date
            """, (account_id,))
            
            monthly_balances = cursor.fetchall()
            
            # Process all monthly balances
            for asset_name, monthly_date, total_balance in monthly_balances:
                if total_balance is not None and total_balance != 0:
                    # Find the asset data for this asset
                    asset_info = next((a for a in asset_data if a[0] == asset_name), None)
                    if asset_info:
                        _, symbol, unit, asset_type = asset_info
                        log_message(f"Monthly balance for {asset_name} at {monthly_date}: {total_balance}")
                        insert_or_update_holdings_monthly(cursor, account_id, asset_name, monthly_date, total_balance, unit, symbol, asset_type)
                else:
                    log_message(f"No balance for {asset_name} at {monthly_date}, skipping")
        else:
            # Original logic for single asset (maintains existing behavior)
            asset_name, symbol, unit, asset_type = asset_data[0]
            log_message(f"Processing monthly holdings for asset: {asset_name}, symbol: {symbol}, unit: {unit}, asset_type: {asset_type}")
            
            # Calculate monthly balances for this asset
            monthly_date = current_date
            while monthly_date <= end_date:
                # Single query to get the balance up to this date
                cursor.execute("""
                    SELECT SUM(credit) - SUM(debit) AS total_balance
                    FROM transactions
                    WHERE account_id = %s AND asset_name = %s AND date < %s AND deleted_at IS NULL
                """, (account_id, asset_name, monthly_date))
                
                result = cursor.fetchone()
                total_balance = result[0] if result else None

                if total_balance is not None and total_balance != 0:
                    log_message(f"Monthly balance for {asset_name} at {monthly_date}: {total_balance}")
                    insert_or_update_holdings_monthly(cursor, account_id, asset_name, monthly_date, total_balance, unit, symbol, asset_type)
                else:
                    log_message(f"No balance for {asset_name} at {monthly_date}, skipping")

                monthly_date += relativedelta(months=1)

        connection.commit()
        log_message(f"Monthly holdings calculated successfully for account_id: {account_id} for assets: {assets}")

    except Exception as e:
        log_message(f"Error while calculating monthly holdings for account_id {account_id}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def calculate_monthly_holdings(account_id, assets, start_date):
    """
    Calculate and update the monthly holdings for each account's asset_name.
    This is the original function kept for backward compatibility.
    """
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # OPTIMIZATION: Single query to get all asset data
        if len(assets) > 1:
            cursor.execute("""
                SELECT DISTINCT asset_name, symbol, unit, asset_type
                FROM transactions
                WHERE account_id = %s AND asset_name = ANY(%s) AND deleted_at IS NULL
            """, (account_id, assets))
            asset_data = cursor.fetchall()
        else:
            # Original logic for single asset
            asset_name = assets[0]
            cursor.execute("""
                SELECT unit, symbol, asset_type
                FROM asset
                WHERE account_id = %s AND asset_name = %s
            """, (account_id, asset_name))
            result = cursor.fetchone()
            if not result:
                log_message(f"Error: Asset '{asset_name}' not found in the asset table for account_id: {account_id}.")
                return
            unit, symbol, asset_type = result
            asset_data = [(asset_name, symbol, unit, asset_type)]

        # OPTIMIZATION: Single query to get all monthly balances for all assets
        if len(asset_data) > 1:
            # Create temporary table for asset names
            cursor.execute("""
                CREATE TEMP TABLE temp_asset_names (asset_name TEXT)
                ON COMMIT DROP
            """)
            
            for asset_name, _, _, _ in asset_data:
                cursor.execute("""
                    INSERT INTO temp_asset_names (asset_name) VALUES (%s)
                """, (asset_name,))
            
            # Generate all monthly dates
            monthly_dates = []
            for asset_name, symbol, unit, asset_type in asset_data:
                # Align start_date to the 1st day of the month
                aligned_start_date = align_to_first_day_of_month(start_date)
                if start_date != aligned_start_date:
                    current_date = aligned_start_date + relativedelta(months=1)
                else:
                    current_date = aligned_start_date

                while current_date <= align_to_first_day_of_month(datetime.utcnow().date()):
                    monthly_dates.append((asset_name, current_date))
                    current_date += relativedelta(months=1)
            
            # Create temporary table for monthly dates
            cursor.execute("""
                CREATE TEMP TABLE temp_monthly_dates (asset_name TEXT, monthly_date DATE)
                ON COMMIT DROP
            """)
            
            for asset_name, monthly_date in monthly_dates:
                cursor.execute("""
                    INSERT INTO temp_monthly_dates (asset_name, monthly_date) VALUES (%s, %s)
                """, (asset_name, monthly_date))
            
            # Single query to get all monthly balances for all assets
            cursor.execute("""
                SELECT 
                    t.asset_name,
                    md.monthly_date,
                    SUM(t.credit) - SUM(t.debit) AS total_balance
                FROM transactions t
                JOIN temp_monthly_dates md ON t.asset_name = md.asset_name
                WHERE t.account_id = %s 
                AND t.deleted_at IS NULL
                AND t.date < md.monthly_date
                GROUP BY t.asset_name, md.monthly_date
                ORDER BY t.asset_name, md.monthly_date
            """, (account_id,))
            
            monthly_balances = cursor.fetchall()
            
            # Process all monthly balances
            for asset_name, monthly_date, total_balance in monthly_balances:
                if total_balance is not None and total_balance != 0:
                    # Find the asset data for this asset
                    asset_info = next((a for a in asset_data if a[0] == asset_name), None)
                    if asset_info:
                        _, symbol, unit, asset_type = asset_info
                        log_message(f"Monthly balance for {asset_name} at {monthly_date}: {total_balance}")
                        insert_or_update_holdings_monthly(cursor, account_id, asset_name, monthly_date, total_balance, unit, symbol, asset_type)
                else:
                    log_message(f"No balance for {asset_name} at {monthly_date}, skipping")
        else:
            # Original logic for single asset (maintains existing behavior)
            asset_name, symbol, unit, asset_type = asset_data[0]
            
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

def publish_transactions_processed(account_id=None, assets=None, total_batches=None, total_assets=None, processing_time_ms=None):
    """
    Publish a Kafka topic indicating that transactions have been processed.
    """
    message_payload = {"status": "transactions_processed"}
    
    # Add batch metadata if provided
    if account_id is not None:
        message_payload["account_id"] = account_id
    if assets is not None:
        message_payload["assets"] = assets
    if total_batches is not None:
        message_payload["totalBatches"] = total_batches
    if total_assets is not None:
        message_payload["totalAssets"] = total_assets
    if processing_time_ms is not None:
        message_payload["processingTimeMs"] = processing_time_ms
    
    publish_kafka_messages(ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS_MONTHLY_COMPLETE, message_payload)

def run(message_payload=None):
    """
    Main function to calculate and update monthly holdings with optimized processing.
    Uses the common utility function for optimized processing.
    """
    log_message("Starting process_transactions_to_holdings_monthly job...")

    if message_payload:
        log_message(f"Received message payload: {message_payload}")

        # Get the earliest transaction date for the account
        account_id = message_payload.get("account_id")
        start_date = get_earliest_transaction_date(account_id)

        # Use the common utility function for optimized processing
        result = process_batch_transactions_optimized(
            message_payload=message_payload,
            holdings_processor_func=None,  # No regular holdings processing for monthly job
            monthly_processor_func=calculate_monthly_holdings_for_specific_assets,
            start_date=start_date
        )
        
        # Publish completion message
        publish_kafka_messages(
            ProducerKafkaTopics.PROCESS_TRANSACTIONS_TO_HOLDINGS_MONTHLY_COMPLETE,
            result
        )
        
        log_message(f"process_transactions_to_holdings_monthly job completed successfully in {result['processingTimeMs']}ms.")

    else:
        log_message("No message payload provided. Processing all accounts and assets.")

        # Use the 1st day of the previous month as the start_date
        start_date = align_to_first_day_of_month(datetime.utcnow().date() - relativedelta(months=1))

        # Retrieve all accounts and their assets
        accounts_and_assets = get_all_accounts_and_assets()
        
        # OPTIMIZED: Process in larger batches for better performance
        batch_size = 100  # Increased from 50 for better throughput
        all_results = []
        
        for i in range(0, len(accounts_and_assets), batch_size):
            batch_accounts_assets = accounts_and_assets[i:i+batch_size]
            log_message(f"Processing batch {i//batch_size+1} with {len(batch_accounts_assets)} account-asset pairs")
            
            # Group by account_id for more efficient processing
            account_assets_map = {}
            for account_id, asset_name in batch_accounts_assets:
                if account_id not in account_assets_map:
                    account_assets_map[account_id] = []
                account_assets_map[account_id].append(asset_name)
            
            # Process each account's assets together
            for account_id, asset_names in account_assets_map.items():
                log_message(f"Processing account_id: {account_id} with {len(asset_names)} assets: {asset_names}")
                calculate_monthly_holdings_for_specific_assets(account_id, asset_names, start_date)
            
            all_results.extend(batch_accounts_assets)
            log_message(f"Completed batch {i//batch_size+1} with {len(batch_accounts_assets)} account-asset pairs.")

        processing_time_ms = int((time.time() - start_time) * 1000)
        total_batches = (len(accounts_and_assets) + batch_size - 1) // batch_size
        
        # Publish completion with batch metadata
        publish_transactions_processed(
            assets=all_results,
            total_batches=total_batches,
            total_assets=len(accounts_and_assets),
            processing_time_ms=processing_time_ms
        )

    log_message(f"process_transactions_to_holdings_monthly job completed successfully. Processed {len(all_results) if 'all_results' in locals() else 0} assets in {total_batches if 'total_batches' in locals() else 0} batches in {processing_time_ms if 'processing_time_ms' in locals() else 0}ms.")