from etl.utils import get_db_connection, log_message, quote_market_data, get_closest_us_market_closing_time
from main import publish_kafka_messages, ProducerKafkaTopics

def validate_symbols(symbols):
    """
    Validate that the provided symbols exist in the holdings table.
    """
    log_message("Validating symbols against the holdings table...")
    log_message(f"Symbols to validate: {symbols}")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Query to check which symbols exist in the holdings table
        cursor.execute("""
            SELECT DISTINCT symbol
            FROM holdings
            WHERE symbol = ANY(%s)
        """.strip(), (symbols,))

        # Extract valid symbols from the query result
        valid_symbols = [row[0] for row in cursor.fetchall()]
        invalid_symbols = set(symbols) - set(valid_symbols)

        if invalid_symbols:
            log_message(f"Warning: The following symbols do not exist in the holdings table and will be ignored: {invalid_symbols}")

        return valid_symbols

    except Exception as e:
        log_message(f"Error validating symbols: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def get_symbols_needing_update(symbols):
    """
    Fetch the list of symbols that need price updates.
    - Symbols that do not exist in the market_data table.
    - Symbols whose updated_at timestamp is earlier than the most recent US market closing time.
    """
    log_message("Fetching symbols that need price updates...")

    # Calculate the most recent US market closing time
    most_recent_closing_time_utc = get_closest_us_market_closing_time()

    log_message(f"Most recent US market closing time in UTC: {most_recent_closing_time_utc}")

    # Query the database
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Fetch symbols that either do not exist in market_data or have outdated timestamps
        cursor.execute("""
            SELECT DISTINCT t.symbol
            FROM transactions t
            LEFT JOIN market_data m ON t.symbol = m.symbol
            WHERE t.symbol = ANY(%s) AND (m.symbol IS NULL OR m.updated_at < %s)
        """, (symbols, most_recent_closing_time_utc))

        # Extract symbols from the query result
        symbols_needing_update = [row[0] for row in cursor.fetchall()]
        log_message(f"Found {len(symbols_needing_update)} symbols needing updates.")
        return symbols_needing_update

    except Exception as e:
        log_message(f"Error fetching symbols needing updates: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def update_asset_prices_in_db(asset_prices):
    """
    Update the database with the latest price data for the symbols.
    Perform an upsert operation to ensure only one row per symbol with the most recent data.
    """
    if not asset_prices:
        log_message("No asset prices provided for database update.")
        return

    log_message("Updating asset prices in the database...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for symbol_data in asset_prices:
            symbol = symbol_data.get("symbol")
            price = symbol_data.get("regularMarketPrice")
            percent_change = symbol_data.get("regularMarketChangePercent", 0)
            updated_at = symbol_data.get("regularMarketTime")

            # Insert or update the market_data table
            cursor.execute("""
                INSERT INTO market_data (symbol, price, percent_change, updated_at)
                VALUES (%s, %s, %s, to_timestamp(%s))
                ON CONFLICT (symbol)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    percent_change = EXCLUDED.percent_change,
                    updated_at = EXCLUDED.updated_at
            """, (symbol, price, percent_change, updated_at))

        connection.commit()
        log_message("Market data updated successfully in the database.")
    except Exception as e:
        connection.rollback()
        log_message(f"Error updating market data in the database: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
    
def publish_price_update_complete(asset_names, asset_names_needing_update):
    """
    Publish a Kafka topic indicating that the price data is ready.
    """
    if not asset_names:
        log_message("No asset names provided for Kafka topic publication.")
        return

    # Use the centralized publish_kafka_messages method
    params = {"assets": asset_names, "updatedAssets": asset_names_needing_update, "status": "complete"}
    publish_kafka_messages(ProducerKafkaTopics.MARKET_DATA_UPDATE_COMPLETE, params)

def run(message_payload):
    """
    Main function to fetch and update asset prices.
    """
    log_message("Starting update_market_data job...")
    log_message(f"Received message payload: {message_payload}")
    
    # Extract the list of symbols from message_content
    if isinstance(message_payload, dict) and "symbols" in message_payload:
        symbols = message_payload["symbols"]
    else:
        log_message("Error: message_payload must be a dictionary with a 'symbols' key.")
        return
    
    log_message(f"Received symbols: {symbols}")

    if not isinstance(symbols, list):
        log_message("Error: symbols must be a list of strings.")
        return
    
    # Validate symbols
    valid_symbols = validate_symbols(symbols)
    if not valid_symbols:
        log_message("No valid symbols found. Exiting job.")
        return
    
    # Determine which symbols need updates
    symbols_needing_update = get_symbols_needing_update(valid_symbols)
    if not symbols_needing_update:
        log_message("No symbols need updates. Exiting job.")

        # Publish Kafka topic
        publish_price_update_complete(symbols, [])

    else:
        log_message(f"Symbols needing updates: {symbols_needing_update}")

        # Quote price data
        asset_prices = quote_market_data(symbols_needing_update)

        # Update the database
        update_asset_prices_in_db(asset_prices)

        # Publish Kafka topic
        publish_price_update_complete(symbols, symbols_needing_update)

    log_message("update_market_data job completed successfully.")