import time
from etl.utils import ( 
    get_db_connection, 
    log_message, 
    get_realtime_stock_data,
    get_realtime_crypto_data,
    get_realtime_forex_data,
    get_closest_us_market_closing_time
)
from datetime import datetime
from main import publish_kafka_messages, ProducerKafkaTopics

def validate_assets(assets):
    """
    Validate that the provided assets exist in the holdings table.
    For now, only validate symbols. Asset type validation will be added later.
    """
    log_message("Validating assets against the holdings table...")
    log_message(f"Assets to validate: {assets}")

    # Extract symbols from the assets list
    symbols = [asset["symbol"] for asset in assets]

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

        # TODO: Add validation for asset_type in the future

        # Return valid assets (filter by valid symbols)
        valid_assets = [asset for asset in assets if asset["symbol"] in valid_symbols]
        return valid_assets

    except Exception as e:
        log_message(f"Error validating assets: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def get_assets_needing_update(assets):
    """
    Fetch the list of assets that need price updates.
    - Assets whose symbols do not exist in the market_data table.
    - Assets whose updated_at timestamp is earlier than the most recent US market closing time.
    """
    log_message("Fetching assets that need price updates...")

    # Extract symbols from the assets list
    symbols = [asset["symbol"] for asset in assets]

    # Calculate the most recent US market closing time
    most_recent_closing_time_utc = get_closest_us_market_closing_time()

    log_message(f"Most recent US market closing time in UTC: {most_recent_closing_time_utc}")

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

        # Filter assets by symbols needing updates
        assets_needing_update = [asset for asset in assets if asset["symbol"] in symbols_needing_update]

        # TODO: Add validation for asset_type in the future

        return assets_needing_update

    except Exception as e:
        log_message(f"Error fetching assets needing updates: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def fetch_and_insert_data(assets):
    """
    Fetch real-time prices for the given assets and insert them into the database.
    Handles API rate limits by retrying failed requests after a delay.
    """
    log_message("Starting fetch_and_insert_data process...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        remaining_assets = assets  # Assets that still need to be fetched
        max_retries = 3  # Maximum number of retries for each asset
        retry_count = 0

        while remaining_assets and retry_count < max_retries:
            successfully_fetched = []  # Track assets successfully fetched in this iteration
            failed_assets = []  # Track assets that failed in this iteration

            for asset in remaining_assets:
                symbol = asset["symbol"]
                asset_type = asset["asset_type"]

                try:
                    log_message(f"Fetching real-time price for symbol: {symbol}, asset_type: {asset_type}...")
                    if asset_type == "STOCK":
                        data = get_realtime_stock_data(symbol)
                    elif asset_type == "CRYPTO":
                        data = get_realtime_crypto_data(symbol)
                    elif asset_type == "FOREX":
                        from_symbol, to_symbol = symbol.split("/")
                        data = get_realtime_forex_data(from_symbol, to_symbol)
                    else:
                        log_message(f"Unsupported asset type: {asset_type} for symbol: {symbol}. Skipping.")
                        continue

                    # Parse the fetched data
                    price = float(data["close"])
                    updated_at = data.get("timestamp", datetime.now().timestamp())
                    percent_change = float(data.get("percent_change", 0))

                    # Insert or update the market_data table
                    cursor.execute("""
                        INSERT INTO market_data (symbol, asset_type, price, percent_change, updated_at)
                        VALUES (%s, %s, %s, %s, to_timestamp(%s))
                        ON CONFLICT (symbol)
                        DO UPDATE SET
                            price = EXCLUDED.price,
                            percent_change = EXCLUDED.percent_change,
                            updated_at = EXCLUDED.updated_at,
                            asset_type = EXCLUDED.asset_type
                    """, (symbol, asset_type, price, percent_change, updated_at))
                    connection.commit()

                    log_message(f"Successfully fetched and stored data for symbol: {symbol}.")
                    successfully_fetched.append(asset)

                except Exception as e:
                    if "429" in str(e):
                        log_message(f"Rate limit exceeded for symbol {symbol}. Retrying in 60 seconds...")
                        failed_assets.append(asset)
                    else:
                        log_message(f"Error fetching data for symbol {symbol}: {e}")

            # Update the remaining assets to only include those that failed
            remaining_assets = failed_assets

            if remaining_assets:
                log_message(f"{len(remaining_assets)} assets failed to fetch. Retrying after 60 seconds...")
                time.sleep(60)  # Wait before retrying
                retry_count += 1

        if remaining_assets:
            log_message(f"Failed to fetch data for {len(remaining_assets)} assets after {max_retries} retries: {remaining_assets}")

    except Exception as e:
        log_message(f"Error during fetch_and_insert_data process: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

    log_message("fetch_and_insert_data process completed.")
    
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
    
    # Extract the list of assets from message_payload
    if isinstance(message_payload, dict) and "assets" in message_payload:
        assets = message_payload["assets"]
    else:
        log_message("Error: message_payload must be a dictionary with an 'assets' key.")
        return
    
    log_message(f"Received assets: {assets}")

    if not isinstance(assets, list):
        log_message("Error: assets must be a list of dictionaries.")
        return
    
    # Validate assets
    valid_assets = validate_assets(assets)
    if not valid_assets:
        log_message("No valid assets found. Exiting job.")
        return
    
    # Determine which assets need updates
    assets_needing_update = get_assets_needing_update(valid_assets)
    if not assets_needing_update:
        log_message("No assets need updates. Exiting job.")

        # Publish Kafka topic
        publish_price_update_complete(assets, [])

    else:
        log_message(f"Assets needing updates: {assets_needing_update}")

        # Fetch and insert data
        fetch_and_insert_data(assets_needing_update)

        # Publish Kafka topic
        publish_price_update_complete(assets, assets_needing_update)

    log_message("update_market_data job completed successfully.")