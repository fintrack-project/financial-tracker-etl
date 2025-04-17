from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from etl.utils import get_db_connection, log_message, load_env_variables, fetch_market_data, get_closest_us_market_closing_time
from main import publish_kafka_messages, ProducerKafkaTopics

# Load environment variables from .env file
env_vars = load_env_variables()

def get_existing_market_average_data(index_names, closest_closing_time):
    """
    Check if market average data exists for the closest US market closing time.
    """
    log_message("Checking existing market average data...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Closest US market closing time: {closest_closing_time}")
        log_message(f"Compare against the previous day: {closest_closing_time - timedelta(days=1)}")
        log_message(f"Index names to check: {index_names}")
        cursor.execute("""
            WITH latest_data AS (
                SELECT symbol, price, price_change, percent_change, price_high, price_low, timestamp,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS row_num
                FROM market_average_data
                WHERE symbol = ANY(%s) AND timestamp >= %s
            )
            SELECT symbol, price, price_change, percent_change, price_high, price_low, timestamp
            FROM latest_data
            WHERE row_num = 1
        """, (index_names, closest_closing_time - timedelta(days=1)))

        existing_data = cursor.fetchall()
        log_message(f"Found {len(existing_data)} existing market average data records.")
        log_message(f"Existing market average data: {existing_data}")
        return [
            {
                "symbol": row[0],
                "price": row[1],
                "price_change": row[2],
                "percent_change": row[3],
                "price_high": row[4],
                "price_low": row[5],
                "timestamp": row[6]
            }
            for row in existing_data
        ]
    except Exception as e:
        log_message(f"Error checking existing market average data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def process_market_data(data):
    """
    Extract price and percent dropped for each symbol.
    """
    processed_data = []
    for symbol_data in data:
        symbol = symbol_data.get("symbol")
        price = symbol_data.get("regularMarketPrice")
        price_change = symbol_data.get("regularMarketChange")
        percent_change = symbol_data.get("regularMarketChangePercent")
        price_high = symbol_data.get("regularMarketDayHigh")
        price_low = symbol_data.get("regularMarketDayLow")
        processed_data.append({
            "symbol": symbol,
            "price": price,
            "price_change": price_change,
            "percent_change": percent_change,
            "price_high": price_high,
            "price_low": price_low
        })
    return processed_data

def save_market_data_to_db(data):
    """
    Save the processed market data into the database.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        for record in data:
            cursor.execute("""
                INSERT INTO market_average_data (symbol, price, price_change, percent_change, price_high, price_low, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                record['symbol'],
                record['price'],
                record['price_change'],
                record['percent_change'],
                record['price_high'],
                record['price_low'],
                datetime.now()
            ))
        connection.commit()
        log_message("Market average data saved successfully in the database.")
    except Exception as e:
        connection.rollback()
        log_message(f"Error saving market average data to the database: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def publish_market_average_data_update_complete(data):
    """
    Publish a Kafka topic indicating that the market average data update is complete.
    """
    log_message(f"Publishing Kafka topic: {ProducerKafkaTopics.MARKET_AVERAGE_DATA_UPDATE_COMPLETE.value}...")
    message_payload = [
        {
            "symbol": record["symbol"],
            "price": record["price"],
            "price_change": record["price_change"],
            "percent_change": record["percent_change"],
            "price_high": record["price_high"],
            "price_low": record["price_low"]
        }
        for record in data
    ]
    publish_kafka_messages(ProducerKafkaTopics.MARKET_AVERAGE_DATA_UPDATE_COMPLETE, message_payload)

def run(index_names):
    print("Running fetch_market_average_data job...")
    """
    Main function to fetch, process, and save market data.
    """

    # Extract the list of asset names if the input is a dictionary
    if isinstance(index_names, dict) and "index_names" in index_names:
        index_names = index_names["index_names"]

    if not isinstance(index_names, list):
        log_message("Error: index_names must be a list of strings.")
        return

    log_message("Starting fetch_market_average_data job...")

    # Determine the closest US market closing time
    closest_closing_time_utc = get_closest_us_market_closing_time()
    log_message(f"Closest US market closing time in UTC: {closest_closing_time_utc}")

    # Check existing data
    existing_data = get_existing_market_average_data(index_names, closest_closing_time_utc)
    log_message(f"There are {len(existing_data)} Existing market average data")
    log_message(f"There are {len(index_names)} index names for market average data")
    if len(existing_data) == len(index_names):
        log_message("All market average data is up-to-date. Using existing data.")
        publish_market_average_data_update_complete(existing_data)

    else:
        log_message("Market average data is not up-to-date. Fetching new data...")

        # Fetch market data and process it if necessary
        raw_data = fetch_market_data(index_names)
        processed_data = process_market_data(raw_data)

        # Save data to the database
        save_market_data_to_db(processed_data)

        # Publish Kafka topic
        publish_market_average_data_update_complete(processed_data)
        log_message("Market data fetched and saved successfully.")

if __name__ == "__main__":
    run()