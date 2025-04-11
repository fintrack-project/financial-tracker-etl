from etl.utils import get_db_connection, log_message, fetch_market_data
from confluent_kafka import Producer
import os
import json

def update_asset_prices_in_db(asset_prices):
    """
    Update the database with the latest price data for the assets.
    """
    log_message("Updating asset prices in the database...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for symbol_data in asset_prices:
            symbol = symbol_data.get("symbol")
            price = symbol_data.get("regularMarketPrice")
            percent_change = symbol_data.get("regularMarketChangePercent", 0)
            timestamp = symbol_data.get("regularMarketTime")
            price_unit = symbol_data.get("currency", "USD")

            # Insert or update the market_data table
            cursor.execute("""
                INSERT INTO market_data (symbol, price, percent_change, timestamp, asset_name, price_unit)
                VALUES (%s, %s, %s, to_timestamp(%s), %s, %s)
                ON CONFLICT (symbol) DO UPDATE
                SET price = EXCLUDED.price,
                    percent_change = EXCLUDED.percent_change,
                    timestamp = EXCLUDED.timestamp,
                    price_unit = EXCLUDED.price_unit
            """, (symbol, price, percent_change, timestamp, symbol, price_unit))

        connection.commit()
        log_message("Market data updated successfully in the database.")
    except Exception as e:
        connection.rollback()
        log_message(f"Error updating market data in the database: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def publish_price_update_complete(asset_names):
    """
    Publish a Kafka topic indicating that the price data is ready.
    """
    log_message("Publishing Kafka topic: ASSET_PRICE_UPDATE_COMPLETE...")
    producer_config = {
        'bootstrap.servers': 'kafka:9093',  # Replace with your Kafka broker address
    }
    producer = Producer(producer_config)

    try:
        message = json.dumps({"assets": asset_names, "status": "complete"})
        producer.produce("ASSET_PRICE_UPDATE_COMPLETE", key="price_update", value=message)
        producer.flush()
        log_message("Published Kafka topic: ASSET_PRICE_UPDATE_COMPLETE")
    except Exception as e:
        log_message(f"Error publishing Kafka topic: {e}")
        raise

def run():
    """
    Main function to fetch and update asset prices.
    """
    log_message("Starting update_asset_prices job...")

    # Fetch assets from the database
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT DISTINCT asset_name FROM transactions")
        asset_names = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()

    # Fetch price data
    asset_prices = fetch_market_data(asset_names)

    # Update the database
    update_asset_prices_in_db(asset_prices)

    # Publish Kafka topic
    publish_price_update_complete(asset_names)

    log_message("update_asset_prices job completed successfully.")