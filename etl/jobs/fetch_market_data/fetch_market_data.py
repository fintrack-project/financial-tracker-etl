import time
from etl.utils import (
    get_db_connection,
    log_message,
    get_realtime_stock_data,
    get_realtime_crypto_data,
    get_realtime_forex_data,
    get_closest_us_market_closing_time
)
from etl.fetch_utils import fetch_and_insert_data
from datetime import datetime, timezone
from main import publish_kafka_messages, ProducerKafkaTopics

def get_assets_needing_update(assets):
    """
    Fetch the list of assets that need price updates from the watchlist_market_data table.
    """
    log_message("Fetching assets that need price updates from watchlist_market_data...")
    symbols = [asset["symbol"] for asset in assets]
    most_recent_closing_time_utc = get_closest_us_market_closing_time()

    log_message(f"Most recent US market closing time in UTC: {most_recent_closing_time_utc}")

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Fetch symbols that either do not exist in watchlist_market_data or have outdated timestamps
        cursor.execute("""
            SELECT DISTINCT m.symbol
            FROM market_data m
            WHERE m.symbol = ANY(%s) AND (m.updated_at IS NULL OR m.updated_at < %s)
        """, (symbols, most_recent_closing_time_utc))

        symbols_needing_update = [row[0] for row in cursor.fetchall()]
        log_message(f"Found {len(symbols_needing_update)} symbols needing updates.")

        return [asset for asset in assets if asset["symbol"] in symbols_needing_update]

    except Exception as e:
        log_message(f"Error fetching assets needing updates: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def insert_or_update_data(cursor, connection, asset, processed_data):
    """
    Insert or update the processed data into the watchlist_market_data table.
    """
    try:
        cursor.execute("""
            INSERT INTO market_data (symbol, asset_type, price, percent_change, change, high, low, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol)
            DO UPDATE SET
                price = EXCLUDED.price,
                percent_change = EXCLUDED.percent_change,
                change = EXCLUDED.change,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                updated_at = EXCLUDED.updated_at,
                asset_type = EXCLUDED.asset_type
        """, (
            asset["symbol"],
            asset["asset_type"],
            processed_data["price"],
            processed_data["percent_change"],
            processed_data["change"],
            processed_data["high"],
            processed_data["low"],
            datetime.now(timezone.utc)
        ))
        connection.commit()
        log_message(f"Successfully inserted or updated data for symbol: {asset['symbol']}")
    except Exception as e:
        log_message(f"Error inserting or updating data for symbol {asset['symbol']}: {e}")
        raise

def run(message_payload):
    """
    Main function to fetch and update asset prices.
    """
    log_message("Starting fetch_watchlist_market_data job...")
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

    # Determine which assets need updates
    assets_needing_update = get_assets_needing_update(assets)
    if not assets_needing_update:
        log_message("No assets need updates. Exiting job.")

        # Publish Kafka topic
        publish_kafka_messages(ProducerKafkaTopics.WATCHLIST_MARKET_DATA_UPDATE_COMPLETE, {"assets": assets, "status": "complete"})
        return

    log_message(f"Assets needing updates: {assets_needing_update}")

    # Fetch and insert data
    required_fields = ["symbol", "price", "percent_change", "change", "high", "low"]
    fetch_and_insert_data(
        assets_needing_update,
        required_fields,
        insert_or_update_data,
        get_realtime_stock_data,
        get_realtime_crypto_data,
        get_realtime_forex_data
    )

    # Publish Kafka topic
    publish_kafka_messages(ProducerKafkaTopics.WATCHLIST_MARKET_DATA_UPDATE_COMPLETE, {"assets": assets, "updatedAssets": assets_needing_update, "status": "complete"})

    log_message("fetch_watchlist_market_data job completed successfully.")