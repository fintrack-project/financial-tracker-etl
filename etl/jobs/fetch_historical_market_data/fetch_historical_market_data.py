import time
from datetime import datetime
from etl.utils import (
    log_message,
    get_historical_stock_data,
    get_historical_crypto_data,
    get_historical_fx_data,
    get_db_connection,
)
from etl.fetch_utils import (
    get_existing_data_ranges,
    adjust_date_range,
    determine_symbols_needing_update,
    handle_api_error
)
from main import publish_kafka_messages, ProducerKafkaTopics


# --- Utility Functions ---
def fetch_and_insert_data(symbols_needing_update, asset_type, max_retries=3, retry_delay=60):
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
            
            while retry_count < max_retries:
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
                    should_retry, error_message = handle_api_error(e, symbol, retry_count, max_retries, retry_delay)
                    if should_retry:
                        retry_count += 1
                        time.sleep(retry_delay)
                    else:
                        log_message(error_message)
                        break

        # Insert the fetched data into the database
        if fetched_data:
            try:
                for data in fetched_data:
                    cursor.execute("""
                        INSERT INTO market_data_monthly (symbol, price, date, asset_type)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (symbol, date)
                        DO UPDATE SET
                            price = EXCLUDED.price,
                            asset_type = EXCLUDED.asset_type
                    """, (
                        data["symbol"],
                        data["price"],
                        data["date"],
                        data["asset_type"]
                    ))
                connection.commit()
                log_message("Successfully inserted historical data into the database.")
            except Exception as e:
                connection.rollback()
                log_message(f"Error inserting historical data into the database: {e}")
                raise

    except Exception as e:
        log_message(f"Error during fetch_and_insert_data process: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

    return fetched_data


def fetch_historical_market_data(symbols, asset_type, start_date, end_date):
    """
    Fetch historical market data for the given symbols, asset type, and date range.
    """
    existing_data_by_symbol = get_existing_data_ranges(symbols, "market_data_monthly", asset_type)
    log_message(f"Existing data ranges: {existing_data_by_symbol}")
    symbols_needing_update = determine_symbols_needing_update(symbols, start_date, end_date, existing_data_by_symbol)
    log_message(f"Symbols and date ranges needing updates: {symbols_needing_update}")
    return fetch_and_insert_data(symbols_needing_update, asset_type)


def publish_market_data_monthly_complete(symbols, asset_type, start_date, end_date, record_count):
    """
    Publish a Kafka topic indicating that the historical market data update is complete.
    """
    message_payload = {
        "symbols": symbols,
        "asset_type": asset_type,
        "start_date": start_date,
        "end_date": end_date,
        "record_count": record_count
    }
    publish_kafka_messages(ProducerKafkaTopics.HISTORICAL_MARKET_DATA_COMPLETE, message_payload)


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