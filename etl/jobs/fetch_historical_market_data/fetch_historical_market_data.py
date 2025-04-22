from utils import (
    log_message
)
from main import publish_kafka_messages, ProducerKafkaTopics
from utils import (
    get_historical_stock_data,
    get_historical_crypto_data,
    get_historical_fx_data,
    log_message,
    get_db_connection,
    get_closest_us_market_closing_time
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def generate_first_dates_of_month(start_date, end_date):
    """
    Generate a list of the first dates of each month between start_date and end_date.
    """
    first_dates = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d").date().replace(day=1)
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current_date <= end_date:
        first_dates.append(current_date)
        current_date += relativedelta(months=1)

    return first_dates


def fetch_historical_market_data(symbols, asset_type, start_date, end_date):
    """
    Fetch historical market data for the given symbols, asset type, and date range.
    Validate existing data in the database and fetch missing data from Alpha Vantage API.
    """
    log_message(f"Fetching historical market data for symbols: {symbols}, asset_type: {asset_type}, start_date: {start_date}, end_date: {end_date}...")

    # Validate input
    if asset_type not in ['stock', 'crypto', 'forex']:
        raise ValueError(f"Unsupported asset_type: {asset_type}. Supported types are 'stock', 'crypto', and 'forex'.")

    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        # Generate all first dates of each month within the date range
        first_dates = generate_first_dates_of_month(start_date, end_date)

        # Check for existing data in the database
        cursor.execute("""
            SELECT symbol, date
            FROM market_data_monthly
            WHERE symbol = ANY(%s) AND asset_type = %s AND date = ANY(%s)
        """, (symbols, asset_type, first_dates))
        existing_data = cursor.fetchall()

        # Organize existing data by symbol and date
        existing_data_by_symbol_and_date = {(row[0], row[1]): True for row in existing_data}
        log_message(f"Found {len(existing_data)} existing records in the database.")

        # Determine symbols and dates needing updates
        symbols_needing_update = []
        for symbol in symbols:
            for date in first_dates:
                if (symbol, date) not in existing_data_by_symbol_and_date:
                    symbols_needing_update.append((symbol, date))

        log_message(f"Symbols and dates needing updates: {symbols_needing_update}")

        # Fetch missing data from Alpha Vantage API
        fetched_data = []
        for symbol, date in symbols_needing_update:
            if asset_type == 'stock':
                api_data = get_historical_stock_data(symbol, start_date=date.strftime("%Y-%m-%d"), end_date=date.strftime("%Y-%m-%d"))
            elif asset_type == 'crypto':
                api_data = get_historical_crypto_data(symbol, market="USD", start_date=date.strftime("%Y-%m-%d"), end_date=date.strftime("%Y-%m-%d"))
            elif asset_type == 'forex':
                from_symbol, to_symbol = symbol.split("/")
                api_data = get_historical_fx_data(from_symbol, to_symbol, start_date=date.strftime("%Y-%m-%d"), end_date=date.strftime("%Y-%m-%d"))
            else:
                log_message(f"Placeholder for asset_type: {asset_type}")
                continue

            # Process and format the fetched data
            for api_date, values in api_data.items():
                us_market_closing_time = get_closest_us_market_closing_time(datetime.strptime(api_date, "%Y-%m-%d"))
                fetched_data.append({
                    "symbol": symbol,
                    "price": float(values.get("4. close") or values.get("4a. close (USD)")),
                    "date": us_market_closing_time.date(),
                    "asset_type": asset_type
                })

        # Insert fetched data into the database
        if fetched_data:
            log_message(f"Inserting {len(fetched_data)} new records into the database...")
            for record in fetched_data:
                cursor.execute("""
                    INSERT INTO market_data_monthly (symbol, price, date, asset_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, date, asset_type) DO NOTHING
                """, (record["symbol"], record["price"], record["date"], record["asset_type"]))
            connection.commit()

        # Combine existing and fetched data
        all_data = existing_data + [
            (record["symbol"], record["price"], record["date"], record["asset_type"])
            for record in fetched_data
        ]

        log_message(f"Successfully fetched and updated historical market data.")
        return all_data

    except Exception as e:
        log_message(f"Error fetching historical market data: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def publish_market_data_monthly_complete(symbols, asset_type, start_date, end_date, record_count):
    """
    Publish a Kafka topic indicating that the historical market data is ready.
    """
    if not symbols:
        log_message("No symbols provided for Kafka topic publication.")
        return

    # Use the centralized publish_kafka_messages method
    params = {
        "symbols": symbols,
        "asset_type": asset_type,
        "start_date": start_date,
        "end_date": end_date,
        "record_count": record_count,
        "status": "complete"
    }
    publish_kafka_messages(ProducerKafkaTopics.MARKET_DATA_MONTHLY_COMPLETE, params)


def run(message_payload):
    """
    Main function to fetch and update historical market data.
    """
    log_message("Starting fetch_historical_market_data job...")
    log_message(f"Received message payload: {message_payload}")

    # Extract the required fields from the message payload
    if isinstance(message_payload, dict):
        symbols = message_payload.get("symbols")
        asset_type = message_payload.get("asset_type")
        start_date = message_payload.get("start_date")
        end_date = message_payload.get("end_date")
    else:
        log_message("Error: message_payload must be a dictionary with 'symbols', 'asset_type', 'start_date', and 'end_date' keys.")
        return

    # Validate the payload
    if not symbols or not asset_type or not start_date or not end_date:
        log_message("Error: Missing required fields in message payload.")
        return

    log_message(f"Received symbols: {symbols}, asset_type: {asset_type}, start_date: {start_date}, end_date: {end_date}")

    if not isinstance(symbols, list):
        log_message("Error: symbols must be a list of strings.")
        return

    # Fetch historical market data
    try:
        data = fetch_historical_market_data(symbols, asset_type, start_date, end_date)
        record_count = len(data)

        # Publish Kafka topic
        publish_market_data_monthly_complete(symbols, asset_type, start_date, end_date, record_count)

    except Exception as e:
        log_message(f"Error during fetch_historical_market_data job: {e}")
        return

    log_message("fetch_historical_market_data job completed successfully.")