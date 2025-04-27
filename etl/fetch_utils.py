import time
from datetime import datetime, timezone
from etl.utils import get_db_connection, log_message

def fetch_data(asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data):
    """
    Fetch real-time data for a given asset.
    """
    symbol = asset["symbol"]
    asset_type = asset["asset_type"]

    try:
        log_message(f"Fetching real-time data for symbol: {symbol}, asset_type: {asset_type}...")
        if asset_type == "STOCK":
            return get_realtime_stock_data(symbol)
        elif asset_type == "CRYPTO":
            return get_realtime_crypto_data(symbol)
        elif asset_type == "FOREX":
            return get_realtime_forex_data(*symbol.split("/"))
        else:
            log_message(f"Unsupported asset type: {asset_type} for symbol: {symbol}. Skipping.")
            return None

    except Exception as e:
        log_message(f"Error fetching data for symbol {symbol}: {e}")
        raise


def process_data(data, required_fields):
    """
    Validate and process the API response data.
    """
    log_message(f"Validating required fields: {required_fields}")
    processed_data = {}
    
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing or invalid field '{field}' in API response: {data}")
        # Extract and process the fields
        processed_data[field] = data[field]
    log_message(f"Processed data: {processed_data}")
    return processed_data

def fetch_and_insert_data(assets, required_fields, insert_or_update_data, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data, max_retries=3, retry_delay=60):
    """
    Fetch, process, and insert data for the given assets.
    Handles API rate limits by retrying failed requests after a delay.

    Args:
        assets (list): List of assets to fetch data for.
        required_fields (list): List of required fields to validate and process.
        insert_or_update_data (function): Job-specific function to insert or update data in the database.
        get_realtime_stock_data (function): Function to fetch stock data.
        get_realtime_crypto_data (function): Function to fetch crypto data.
        get_realtime_forex_data (function): Function to fetch forex data.
        max_retries (int): Maximum number of retries for failed requests.
        retry_delay (int): Delay in seconds before retrying after a rate limit error.
    """
    log_message("Starting fetch_and_insert_data process...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        remaining_assets = assets
        retry_count = 0

        while remaining_assets and retry_count < max_retries:
            successfully_fetched = []
            failed_assets = []

            for asset in remaining_assets:
                symbol = asset["symbol"]
                asset_type = asset["asset_type"]

                try:
                    # Fetch data
                    data = fetch_data(asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)
                    if not data:
                        failed_assets.append(asset)
                        continue

                    # Process data
                    processed_data = process_data(data, required_fields)

                    # Insert or update data
                    insert_or_update_data(cursor, connection, asset, processed_data)

                    successfully_fetched.append(asset)

                except Exception as e:
                    if "429" in str(e):
                        log_message(f"Rate limit exceeded for symbol {symbol}. Retrying in {retry_delay} seconds...")
                        failed_assets.append(asset)
                    else:
                        log_message(f"Error processing data for symbol {symbol}: {e}")
                        failed_assets.append(asset)

            remaining_assets = failed_assets

            if remaining_assets:
                log_message(f"{len(remaining_assets)} assets failed. Retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_count += 1

        if remaining_assets:
            log_message(f"Failed to fetch data for {len(remaining_assets)} assets after {max_retries} retries.")

    except Exception as e:
        log_message(f"Error during fetch_and_insert_data process: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

    log_message("fetch_and_insert_data process completed.")