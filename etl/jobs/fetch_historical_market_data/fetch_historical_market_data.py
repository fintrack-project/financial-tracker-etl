import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from utils import (
    log_message,
    get_historical_stock_data,
    get_historical_crypto_data,
    get_historical_fx_data,
    get_db_connection,
)
from main import publish_kafka_messages, ProducerKafkaTopics


# --- Utility Functions ---
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


def fetch_existing_data_ranges(symbols, asset_type):
    """
    Fetch all available dates from the database for the given symbols and asset type.
    Returns a dict: {symbol: set([date1, date2, ...]), ...}
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT symbol, date
            FROM market_data_monthly
            WHERE symbol = ANY(%s) AND asset_type = %s
        """, (symbols, asset_type))
        rows = cursor.fetchall()
        data_by_symbol = {}
        for symbol, date in rows:
            data_by_symbol.setdefault(symbol, set()).add(date)
        return data_by_symbol
    finally:
        cursor.close()
        connection.close()


def determine_symbols_needing_update(symbols, asset_type, start_date, end_date, existing_data_by_symbol):
    """
    Determine which symbols and date ranges need updates based on existing data.
    - Only fetch missing months to reduce API calls
    """
    from dateutil.relativedelta import relativedelta
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


def fetch_and_insert_data(symbols_needing_update, asset_type):
    """
    Fetch missing data from the Twelve Data API and insert it into the database.
    """
    fetched_data = []
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for symbol, range_start, range_end in symbols_needing_update:
            log_message(f"Processing symbol {symbol} with range: start_date={range_start}, end_date={range_end}")
            adjusted_range_start, adjusted_range_end, fetch_current_month_only = adjust_date_range(range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            log_message(f"Adjusted range for symbol {symbol}: start_date={adjusted_range_start}, end_date={adjusted_range_end}, fetch_current_month_only={fetch_current_month_only}")
            retry_count = 0
            while retry_count < 3:
                try:
                    log_message(f"Fetching data for symbol {symbol} from {adjusted_range_start} to {adjusted_range_end}...")
                    if asset_type == 'STOCK':
                        api_data = get_historical_stock_data(symbol, start_date=adjusted_range_start, end_date=adjusted_range_end)
                    elif asset_type == 'CRYPTO':
                        api_data = get_historical_crypto_data(symbol, market="USD", start_date=adjusted_range_start, end_date=adjusted_range_end)
                    elif asset_type == 'FOREX':
                        from_symbol, to_symbol = symbol.split('/')
                        api_data = get_historical_fx_data(from_symbol, to_symbol, start_date=adjusted_range_start, end_date=adjusted_range_end)
                    else:
                        log_message(f"Unsupported asset type: {asset_type}")
                        break

                    # Process and format the fetched data
                    if fetch_current_month_only:
                        # Only process the last entry in the API response
                        if api_data:
                            last_entry = api_data[-1]  # Get the last data point
                            api_date = last_entry["datetime"]
                            fetched_data.append({
                                "symbol": symbol,
                                "price": float(last_entry["close"]),
                                "date": datetime.strptime(api_date, "%Y-%m-%d").date(),  # Use the date as-is
                                "asset_type": asset_type
                            })
                            log_message(f"Fetched data (current month only): {fetched_data[-1]}")
                    else:
                        # Process all entries in the API response
                        for entry in api_data:
                            api_date = entry["datetime"]
                            fetched_data.append({
                                "symbol": symbol,
                                "price": float(entry["close"]),
                                "date": datetime.strptime(api_date, "%Y-%m-%d").date(),  # Use the date as-is
                                "asset_type": asset_type
                            })

                    log_message(f"Successfully fetched historical data for symbol: {symbol}.")
                    log_message(f"Fetched {len(fetched_data)} records for symbol: {symbol} from {range_start} to {range_end}.")
                    for data in fetched_data:
                        log_message(f"Fetched data: {data}")
                    break
                except Exception as e:
                    error_str = str(e)
                    if "429" in error_str:
                        log_message(f"Rate limit exceeded for symbol {symbol}. Retrying in 60 seconds...")
                        time.sleep(60)
                        retry_count += 1
                    elif "404" in error_str or "code': 404" in error_str:
                        log_message(f"Symbol {symbol} not found (404 error). Skipping this symbol.")
                        break  # No need to retry for non-existent symbols
                    else:
                        log_message(f"Error fetching data for symbol {symbol}: {e}")
                        break

        if fetched_data:
            log_message(f"Inserting {len(fetched_data)} new records into the database...")
            for record in fetched_data:
                cursor.execute("""
                    INSERT INTO market_data_monthly (symbol, price, date, asset_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, date, asset_type) DO NOTHING
                """, (record["symbol"], record["price"], record["date"], record["asset_type"]))
            connection.commit()

    finally:
        cursor.close()
        connection.close()

    return fetched_data


def fetch_historical_market_data(symbols, asset_type, start_date, end_date):
    """
    Fetch historical market data for the given symbols, asset type, and date range.
    """
    existing_data_by_symbol = fetch_existing_data_ranges(symbols, asset_type)
    log_message(f"Existing data ranges: {existing_data_by_symbol}")
    symbols_needing_update = determine_symbols_needing_update(symbols, asset_type, start_date, end_date, existing_data_by_symbol)
    log_message(f"Symbols and date ranges needing updates: {symbols_needing_update}")
    return fetch_and_insert_data(symbols_needing_update, asset_type)


def publish_market_data_monthly_complete(symbols, asset_type, start_date, end_date, record_count):
    """
    Publish a Kafka topic indicating that the historical market data is ready.
    """
    if not symbols:
        log_message("No symbols provided for Kafka topic publication.")
        return

    params = {
        "symbols": symbols,
        "asset_type": asset_type,
        "start_date": start_date,
        "end_date": end_date,
        "record_count": record_count,
        "status": "complete"
    }
    publish_kafka_messages(ProducerKafkaTopics.HISTORICAL_MARKET_DATA_COMPLETE, params)


def run(message_payload):
    """
    Main function to fetch and update historical market data for mixed asset types.
    """
    log_message("Starting fetch_historical_market_data job...")
    log_message(f"Received message payload: {message_payload}")

    if not isinstance(message_payload, dict):
        log_message("Error: message_payload must be a dictionary with 'assets', 'start_date', and 'end_date' keys.")
        return

    assets = message_payload.get("assets")
    start_date = message_payload.get("start_date")
    end_date = message_payload.get("end_date")

    if not assets or not start_date or not end_date:
        log_message("Error: Missing required fields in message payload.")
        return

    log_message(f"Received assets: {assets}, start_date: {start_date}, end_date: {end_date}")

    assets_by_asset_type = {}
    for item in assets:
        if not isinstance(item, dict) or "symbol" not in item or "asset_type" not in item:
            log_message(f"Invalid symbol entry: {item}. Skipping.")
            continue
        asset_type = item["asset_type"]
        symbol = item["symbol"]
        assets_by_asset_type.setdefault(asset_type, []).append(symbol)

    log_message(f"Grouped assets by asset type: {assets_by_asset_type}")

    try:
        total_record_count = 0
        for asset_type, asset_symbols in assets_by_asset_type.items():
            log_message(f"Processing asset type: {asset_type} with symbols: {asset_symbols}")
            data = fetch_historical_market_data(asset_symbols, asset_type, start_date, end_date)
            record_count = len(data)
            total_record_count += record_count
            publish_market_data_monthly_complete(asset_symbols, asset_type, start_date, end_date, record_count)

        log_message(f"Total records processed: {total_record_count}")

    except Exception as e:
        log_message(f"Error during fetch_historical_market_data job: {e}")
        return

    log_message("fetch_historical_market_data job completed successfully.")