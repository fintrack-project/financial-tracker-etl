from etl.utils import get_db_connection, log_message, fetch_market_data
from confluent_kafka import Producer
from main import publish_kafka_messages, ProducerKafkaTopics
from datetime import datetime, timedelta
import pytz

def validate_asset_names(asset_names):
    """
    Validate that the provided asset_names exist in the holdings table.
    """
    log_message("Validating asset names against the holdings table...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Query to check which asset_names exist in the holdings table
        cursor.execute("""
            SELECT DISTINCT asset_name
            FROM holdings
            WHERE asset_name = ANY(%s)
        """, (asset_names,))

        # Extract valid asset names from the query result
        valid_asset_names = [row[0] for row in cursor.fetchall()]
        invalid_asset_names = set(asset_names) - set(valid_asset_names)

        if invalid_asset_names:
            log_message(f"Warning: The following asset names do not exist in the holdings table and will be ignored: {invalid_asset_names}")

        return valid_asset_names

    except Exception as e:
        log_message(f"Error validating asset names: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def get_assets_needing_update(asset_names):
    """
    Fetch the list of assets that need price updates.
    - Assets that do not exist in the market_data table.
    - Assets whose timestamp is earlier than the most recent US market closing time.
    """
    log_message("Fetching assets that need price updates...")

    # Calculate the most recent US market closing time
    eastern = pytz.timezone("US/Eastern")
    utc = pytz.utc
    now = datetime.now(eastern)

    if now.hour < 16:  # Before today's market closing time
        # Use yesterday's closing time
        most_recent_closing_time = (now - timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    else:  # After today's market closing time
        # Use today's closing time
        most_recent_closing_time = now.replace(hour=16, minute=0, second=0, microsecond=0)

    # Convert to UTC for database comparison
    most_recent_closing_time_utc = most_recent_closing_time.astimezone(utc)

    log_message(f"Most recent US market closing time in UTC: {most_recent_closing_time_utc}")

    # Query the database
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Fetch assets that either do not exist in market_data or have outdated timestamps
        cursor.execute("""
            SELECT DISTINCT t.asset_name
            FROM transactions t
            LEFT JOIN market_data m ON t.asset_name = m.symbol
            WHERE t.asset_name = ANY(%s) AND (m.symbol IS NULL OR m.timestamp < %s)
        """, (asset_names, most_recent_closing_time_utc))

        # Extract asset names from the query result
        assets_needing_update = [row[0] for row in cursor.fetchall()]
        log_message(f"Found {len(assets_needing_update)} assets needing updates.")
        return assets_needing_update

    except Exception as e:
        log_message(f"Error fetching assets needing updates: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def update_asset_prices_in_db(asset_prices):
    """
    Update the database with the latest price data for the assets.
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
    if not asset_names:
        log_message("No asset names provided for Kafka topic publication.")
        return

    # Use the centralized publish_kafka_messages method
    params = {"assets": asset_names, "status": "complete"}
    publish_kafka_messages(ProducerKafkaTopics.ASSET_PRICE_UPDATE_COMPLETE, params)

def run(asset_names):
    """
    Main function to fetch and update asset prices.
    """
    log_message("Starting update_asset_prices job...")

    # Step 1: Validate asset names
    valid_asset_names = validate_asset_names(asset_names)
    if not valid_asset_names:
        log_message("No valid asset names found. Exiting job.")
        return
    
    # Step 2: Determine which assets need updates
    asset_names_needing_update = get_assets_needing_update(valid_asset_names)
    if not asset_names_needing_update:
        log_message("No assets need updates. Exiting job.")
        return

    # Step 3: Fetch price data
    asset_prices = fetch_market_data(asset_names_needing_update)

    # Step 4: Update the database
    update_asset_prices_in_db(asset_prices)

    # Step 5: Publish Kafka topic
    publish_price_update_complete(asset_names)

    log_message("update_asset_prices job completed successfully.")