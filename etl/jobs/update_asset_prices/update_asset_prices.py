import requests
from etl.utils import get_db_connection, log_message
from confluent_kafka import Producer
import os
import json

def fetch_market_data(asset_names):
    """
    Fetch the latest price data for the given assets from an external API.
    """
    api_url = os.getenv("RAPIDAPI_URL")
    api_key = os.getenv("RAPIDAPI_KEY")

    if not api_url or not api_key:
        raise ValueError("API_URL or API_KEY is not set. Check your .env file.")

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
    params = {"symbols": ",".join(asset_names)}

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "quoteResponse" not in data or "result" not in data["quoteResponse"]:
            raise ValueError("Invalid API response format")

        # Extract relevant fields for each asset
        asset_prices = {}
        for asset in data["quoteResponse"]["result"]:
            symbol = asset.get("symbol")
            price = asset.get("regularMarketPrice")
            percent_change = asset.get("regularMarketChangePercent", 0)  # Default to 0 if not provided
            timestamp = asset.get("regularMarketTime")  # Unix timestamp
            price_unit = asset.get("currency", "USD")  # Default to USD if not provided

            if symbol and price is not None:
                asset_prices[symbol] = {
                    "price": price,
                    "percent_change": percent_change,
                    "timestamp": timestamp,
                    "price_unit": price_unit
                }

        log_message(f"Successfully fetched market data for {len(asset_prices)} assets.")
        return asset_prices

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching market data: {e}")
        raise

def update_asset_prices_in_db(asset_prices):
    """
    Update the database with the latest price data for the assets.
    """
    log_message("Updating asset prices in the database...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        for symbol, price_data in asset_prices.items():
            # Extract relevant fields from the parsed API response
            price = price_data["price"]
            percent_change = price_data["percent_change"]
            timestamp = price_data["timestamp"]
            price_unit = price_data["price_unit"]

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