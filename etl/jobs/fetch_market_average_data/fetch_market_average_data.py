from datetime import datetime
from dotenv import load_dotenv
from etl.utils import get_db_connection, log_message, load_env_variables, fetch_market_data
from main import publish_kafka_messages, ProducerKafkaTopics

# Load environment variables from .env file
env_vars = load_env_variables()

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
    # Step 1: Fetch market data
    raw_data = fetch_market_data(index_names)

    # Step 2: Process market data
    processed_data = process_market_data(raw_data)

    # Step 3: Save data to the database
    save_market_data_to_db(processed_data)

    # Step 4: Publish Kafka topic
    publish_market_average_data_update_complete(processed_data)

    log_message("Market data fetched and saved successfully.")

if __name__ == "__main__":
    run()