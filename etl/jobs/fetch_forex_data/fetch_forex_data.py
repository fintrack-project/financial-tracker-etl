import time
from datetime import datetime, timezone
from etl.utils import (
    get_db_connection,
    log_message,
    get_realtime_forex_data,
    get_closest_us_market_closing_time
)
from main import publish_kafka_messages, ProducerKafkaTopics


def get_forex_symbols_needing_update(symbols):
    """
    Check which forex symbols need updates.
    - Symbols that do not exist in the forex_data table.
    - Symbols whose updated_at timestamp is earlier than the most recent US market closing time.
    """
    log_message("Checking which forex symbols need updates...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Get the most recent US market closing time
        most_recent_closing_time_utc = get_closest_us_market_closing_time()
        log_message(f"Most recent US market closing time (UTC): {most_recent_closing_time_utc}")

        # Query to check which symbols need updates
        cursor.execute("""
            SELECT DISTINCT symbol
            FROM forex_data
            WHERE symbol = ANY(%s) AND (updated_at IS NULL OR updated_at < %s)
        """, (symbols, most_recent_closing_time_utc))

        # Extract symbols needing updates
        symbols_needing_update = [row[0] for row in cursor.fetchall()]
        log_message(f"Symbols needing updates: {symbols_needing_update}")

        return symbols_needing_update

    except Exception as e:
        log_message(f"Error checking forex symbols needing updates: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


def fetch_data(symbol):
    """
    Fetch real-time data for a given forex symbol.
    """

    try:
        log_message(f"Fetching real-time forex data for symbol: {symbol}...")
        data = get_realtime_forex_data(*symbol.split("/"))
        log_message(f"API response for symbol {symbol}: {data}")
        return data

    except Exception as e:
        log_message(f"Error fetching forex data for symbol {symbol}: {e}")
        raise


def process_data(data, symbol):
    """
    Validate and process the API response data.
    """
    # Validate the API response
    required_fields = ["close", "percent_change"]
    log_message(f"Validating required fields: {required_fields}")
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing or invalid field '{field}' in API response for symbol {symbol}: {data}")

    log_message(f"All required fields are present in the API response for symbol {symbol}.")

    # Extract and process the fields
    price = float(data["close"])  # Convert "close" to float
    percent_change = float(data["percent_change"]) if data["percent_change"] else 0.0  # Convert "percent_change" to float or default to 0.0

    log_message(f"Extracted data for symbol {symbol}: price={price}, percent_change={percent_change}")
    return price, percent_change


def insert_or_update_data(cursor, connection, symbol, price, percent_change):
    """
    Insert or update the processed forex data into the database.
    """
    try:
        # Insert or update the forex_data table
        cursor.execute("""
            INSERT INTO forex_data (symbol, price, percent_change, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol)
            DO UPDATE SET
                price = EXCLUDED.price,
                percent_change = EXCLUDED.percent_change,
                updated_at = EXCLUDED.updated_at
        """, (symbol, price, percent_change, datetime.now(timezone.utc)))
        connection.commit()

        log_message(f"Successfully inserted or updated forex data for symbol: {symbol}.")
    except Exception as e:
        log_message(f"Error inserting or updating forex data for symbol {symbol}: {e}")
        raise


def fetch_and_insert_data(symbols):
    """
    Fetch real-time forex data for the given symbols and insert them into the database.
    Handles API rate limits by retrying failed requests after a delay.
    """
    log_message("Starting fetch_and_insert_data process...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        remaining_symbols = symbols  # Symbols that still need to be fetched
        max_retries = 3  # Maximum number of retries for each asset
        retry_count = 0

        while remaining_symbols and retry_count < max_retries:
            successfully_fetched = []  # Track symbols successfully fetched in this iteration
            failed_symbols = []  # Track symbols that failed in this iteration

            for symbol in remaining_symbols:

                try:
                    # Fetch data
                    data = fetch_data(symbol)
                    if not data:
                        failed_symbols.append(symbol)
                        continue

                    # Process data
                    price, percent_change = process_data(data, symbol)

                    # Insert or update data
                    insert_or_update_data(cursor, connection, symbol, price, percent_change)

                    successfully_fetched.append(symbol)

                except Exception as e:
                    if "429" in str(e):
                        log_message(f"Rate limit exceeded for symbol {symbol}. Retrying in 60 seconds...")
                        failed_symbols.append(symbol)
                    else:
                        log_message(f"Error processing forex data for symbol {symbol}: {e}")
                        failed_symbols.append(symbol)

            # Update the remaining symbols to only include those that failed
            remaining_symbols = failed_symbols

            if remaining_symbols:
                log_message(f"{len(remaining_symbols)} symbols failed to fetch. Retrying after 60 seconds...")
                time.sleep(60)  # Wait before retrying
                retry_count += 1

        if remaining_symbols:
            log_message(f"Failed to fetch data for {len(remaining_symbols)} symbols after {max_retries} retries: {remaining_symbols}")

    except Exception as e:
        log_message(f"Error during fetch_and_insert_data process: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

    log_message("fetch_and_insert_data process completed.")


def publish_forex_update_complete(asset_names, asset_names_needing_update):
    """
    Publish a Kafka topic indicating that the forex data is ready.
    """
    if not asset_names:
        log_message("No asset names provided for Kafka topic publication.")
        return

    # Use the centralized publish_kafka_messages method
    params = {"assets": asset_names, "updatedAssets": asset_names_needing_update, "status": "complete"}
    publish_kafka_messages(ProducerKafkaTopics.FOREX_DATA_UPDATE_COMPLETE, params)

def run(message_payload):
    """
    Main function to handle the fetch_forex_data process.
    """
    log_message("Starting fetch_forex_data job...")
    log_message(f"Received message payload: {message_payload}")

    # Extract symbols and asset type from the payload
    if not isinstance(message_payload, dict) or "symbols" not in message_payload or "asset_type" not in message_payload:
        log_message("Invalid payload format. Must contain 'symbols' and 'asset_type'.")
        return

    symbols = message_payload["symbols"]
    asset_type = message_payload["asset_type"]

    if asset_type != "FOREX":
        log_message(f"Unsupported asset type: {asset_type}. Only 'FOREX' is supported.")
        return

    # Get symbols needing updates
    symbols_needing_update = get_forex_symbols_needing_update(symbols)
    if not symbols_needing_update:
        log_message("No forex symbols need updates. Exiting process.")
        publish_kafka_messages(ProducerKafkaTopics.FOREX_DATA_UPDATE_COMPLETE, {"symbols": symbols, "status": "complete"})
        return

    log_message(f"Symbols needing updates: {symbols_needing_update}")

    if not symbols_needing_update:
        log_message("No forex symbols need updates. Exiting process.")

        # Publish Kafka topic
        publish_forex_update_complete(symbols, [])

    else:
        log_message(f"Symbols needing updates: {symbols_needing_update}")

        # Fetch and insert data
        fetch_and_insert_data(symbols_needing_update)

        # Publish Kafka topic
        publish_forex_update_complete(symbols, symbols_needing_update)

    log_message("update_market_data job completed successfully.")