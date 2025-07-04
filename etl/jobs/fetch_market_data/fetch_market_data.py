import time
from etl.utils import (
    get_db_connection,
    log_message,
    get_realtime_stock_data,
    get_realtime_crypto_data,
    get_realtime_forex_data
)
from etl.fetch_utils import (
    fetch_and_insert_data,
    get_existing_data
)
from datetime import datetime, timezone
from main import publish_kafka_messages, ProducerKafkaTopics

def get_assets_needing_update(assets):
    """
    Fetch the list of assets that need price updates from the market_data table.
    Now only checks if the symbol exists in the database, as the frontend handles refresh cycles.
    """
    log_message("Fetching assets that need price updates from market_data...")
    symbols = [asset["symbol"] for asset in assets]

    # Get existing symbols from market_data table
    existing_symbols = get_existing_data(symbols, "market_data")
    symbols_needing_update = [symbol for symbol in symbols if symbol not in existing_symbols]
    log_message(f"Found {len(symbols_needing_update)} symbols needing updates.")

    return [asset for asset in assets if asset["symbol"] in symbols_needing_update]

def insert_or_update_data(cursor, connection, asset, processed_data):
    """
    Insert or update the processed data into the market_data table.
    """
    try:
        cursor.execute("""
            INSERT INTO market_data (symbol, asset_type, price, percent_change, change, high, low, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, asset_type)
            DO UPDATE SET
                price = EXCLUDED.price,
                percent_change = EXCLUDED.percent_change,
                change = EXCLUDED.change,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                updated_at = EXCLUDED.updated_at
        """, (
            asset["symbol"],
            asset["asset_type"],
            float(processed_data["close"]),
            float(processed_data["percent_change"]),
            float(processed_data["change"]),
            float(processed_data["high"]),
            float(processed_data["low"]),
            datetime.now(timezone.utc)
        ))
        connection.commit()
        log_message(f"Successfully inserted or updated data for symbol: {asset['symbol']}")
    except Exception as e:
        log_message(f"Error inserting or updating data for symbol {asset['symbol']}: {e}")
        raise

def run(message_payload):
    """
    Main function to fetch and update asset prices with batching.
    """
    log_message("Starting fetch_market_data job...")
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
        publish_kafka_messages(ProducerKafkaTopics.MARKET_DATA_UPDATE_COMPLETE, {"assets": assets, "status": "complete"})
        return

    log_message(f"Assets needing updates: {assets_needing_update}")

    # Batching logic
    batch_size = 100
    all_results = []
    required_fields = ["symbol", "close", "percent_change", "change", "high", "low"]
    start_time = time.time()
    for i in range(0, len(assets_needing_update), batch_size):
        batch = assets_needing_update[i:i+batch_size]
        fetch_and_insert_data(
            batch,
            required_fields,
            insert_or_update_data,
            get_realtime_stock_data,
            get_realtime_crypto_data,
            get_realtime_forex_data
        )
        all_results.extend(batch)
        log_message(f"Processed batch {i//batch_size+1} with {len(batch)} assets.")

    processing_time_ms = int((time.time() - start_time) * 1000)
    total_batches = (len(assets_needing_update) + batch_size - 1) // batch_size
    publish_kafka_messages(
        ProducerKafkaTopics.MARKET_DATA_UPDATE_COMPLETE,
        {"assets": all_results, "totalBatches": total_batches, "totalTransactions": len(assets_needing_update), "processingTimeMs": processing_time_ms, "status": "complete"}
    )
    log_message("fetch_market_data job completed successfully.")