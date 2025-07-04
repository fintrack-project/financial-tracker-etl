import time
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
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

def get_existing_data(symbols, table_name, asset_type=None):
    """
    Check if data exists for the given symbols in the specified table.
    Returns a list of existing symbols.
    """
    log_message(f"Checking existing data in {table_name}...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        if asset_type:
            cursor.execute(f"""
                SELECT symbol
                FROM {table_name}
                WHERE symbol = ANY(%s) AND asset_type = %s
            """, (symbols, asset_type))
        else:
            cursor.execute(f"""
                SELECT symbol
                FROM {table_name}
                WHERE symbol = ANY(%s)
            """, (symbols,))

        existing_symbols = [row[0] for row in cursor.fetchall()]
        log_message(f"Found {len(existing_symbols)} existing symbols in {table_name}.")
        return existing_symbols

    except Exception as e:
        log_message(f"Error checking existing data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def get_existing_data_ranges(symbols, table_name, asset_type=None):
    """
    Fetch all available dates from the database for the given symbols and asset type.
    Returns a dict: {symbol: set([date1, date2, ...]), ...}
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        if asset_type:
            cursor.execute(f"""
                SELECT symbol, date
                FROM {table_name}
                WHERE symbol = ANY(%s) AND asset_type = %s
            """, (symbols, asset_type))
        else:
            cursor.execute(f"""
                SELECT symbol, date
                FROM {table_name}
                WHERE symbol = ANY(%s)
            """, (symbols,))

        rows = cursor.fetchall()
        data_by_symbol = {}
        for symbol, date in rows:
            data_by_symbol.setdefault(symbol, set()).add(date)
        return data_by_symbol
    finally:
        cursor.close()
        connection.close()

def adjust_date_range(start_date, end_date):
    """
    Adjust the start_date and end_date to handle edge cases:
    - Ensure start_date is the first day of its month.
    - Ensure the interval is at least 1 month.
    - Include the first day of the current month if the current date is after the first day.
    - Prevent requests for future dates.
    """
    # Set start_date to the first day of its month
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date().replace(day=1)
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    current_date = datetime.now().date()

    if end_date_obj >= current_date.replace(day=1):
        end_date_obj = current_date

    # Handle special case: start_date and end_date are the same
    fetch_current_month_only = (start_date_obj == end_date_obj)

    return start_date_obj.strftime("%Y-%m-%d"), end_date_obj.strftime("%Y-%m-%d"), fetch_current_month_only

def determine_symbols_needing_update(symbols, start_date, end_date, existing_data_by_symbol):
    """
    Determine which symbols and date ranges need updates based on existing data.
    - Only fetch missing months to reduce API calls
    """
    symbols_needing_update = []
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date().replace(day=1)
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date().replace(day=1)
    
    def month_range(start, end):
        months = []
        current = start
        while current <= end:
            months.append(current)
            current += relativedelta(months=1)
        return months

    for symbol in symbols:
        expected_dates = set(month_range(start_date_obj, end_date_obj))
        existing_dates = existing_data_by_symbol.get(symbol, set())
        missing_dates = expected_dates - existing_dates
        
        if not missing_dates:
            log_message(f"Data for symbol {symbol} is fully covered in the database. Skipping API call.")
            continue
        else:
            log_message(f"Symbol {symbol} is missing data for months: {sorted(missing_dates)}")
            # Fetch the full missing range (from min to max missing month)
            symbols_needing_update.append((symbol, min(missing_dates), max(missing_dates)))

    return symbols_needing_update

def handle_api_error(error, symbol, retry_count, max_retries, retry_delay):
    """
    Handle API errors with specific handling for rate limits (429) and not found (404) errors.
    Returns a tuple of (should_retry, error_message)
    """
    error_str = str(error)
    
    if "429" in error_str:
        if retry_count < max_retries:
            log_message(f"Rate limit exceeded for symbol {symbol}. Retrying in {retry_delay} seconds...")
            return True, f"Rate limit exceeded for symbol {symbol}"
        else:
            return False, f"Rate limit exceeded for symbol {symbol} after {max_retries} retries"
    
    elif "404" in error_str or "code': 404" in error_str:
        log_message(f"Symbol {symbol} not found (404 error). Skipping this symbol.")
        return False, f"Symbol {symbol} not found"
    
    else:
        if retry_count < max_retries:
            log_message(f"Error fetching data for symbol {symbol}: {error}. Retrying...")
            return True, f"Error fetching data for symbol {symbol}: {error}"
        else:
            return False, f"Failed to fetch data for symbol {symbol} after {max_retries} attempts: {error}"