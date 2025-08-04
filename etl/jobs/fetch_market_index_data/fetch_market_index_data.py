from datetime import datetime, timezone
import time
from etl.utils import get_db_connection, log_message, load_env_variables, quote_market_index_data
from etl.fetch_utils import get_existing_data
from etl.main import publish_kafka_messages, ProducerKafkaTopics

# Load environment variables from .env file
env_vars = load_env_variables()

def get_existing_market_index_data(symbols):
    """
    Check if market index data exists for the given symbols.
    Now only checks if the symbols exist in the database, as the frontend handles refresh cycles.
    """
    log_message("Checking existing market index data...")
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        log_message(f"Symbols to check: {symbols}")
        cursor.execute("""
            SELECT symbol, price, price_change, percent_change, price_high, price_low, updated_at
            FROM market_index_data
            WHERE symbol = ANY(%s)
        """, (symbols,))

        existing_data = cursor.fetchall()
        log_message(f"Found {len(existing_data)} existing market index data records.")
        log_message(f"Existing market index data: {existing_data}")
        return [
            {
                "symbol": row[0],
                "price": float(row[1]),
                "price_change": float(row[2]),
                "percent_change": float(row[3]),
                "price_high": float(row[4]),
                "price_low": float(row[5]),
                "updated_at": row[6]
            }
            for row in existing_data
        ]
    except Exception as e:
        log_message(f"Error checking existing market index data: {e}")
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
                INSERT INTO market_index_data (symbol, price, price_change, percent_change, price_high, price_low, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    price_change = EXCLUDED.price_change,
                    percent_change = EXCLUDED.percent_change,
                    price_high = EXCLUDED.price_high,
                    price_low = EXCLUDED.price_low,
                    updated_at = EXCLUDED.updated_at
            """, (
                record['symbol'],
                record['price'],
                record['price_change'],
                record['percent_change'],
                record['price_high'],
                record['price_low'],
                datetime.now(timezone.utc)
            ))
        connection.commit()
        log_message("Market index data saved successfully in the database.")
    except Exception as e:
        connection.rollback()
        log_message(f"Error saving market index data to the database: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def publish_market_index_data_update_complete(data, total_batches=None, total_symbols=None, processing_time_ms=None):
    """
    Publish a Kafka topic indicating that the market index data update is complete.
    """
    message_payload = {
        "symbols": [
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
    }
    
    # Add batch metadata if provided
    if total_batches is not None:
        message_payload["totalBatches"] = total_batches
    if total_symbols is not None:
        message_payload["totalSymbols"] = total_symbols
    if processing_time_ms is not None:
        message_payload["processingTimeMs"] = processing_time_ms
    message_payload["status"] = "complete"
    
    publish_kafka_messages(ProducerKafkaTopics.MARKET_INDEX_DATA_UPDATE_COMPLETE, message_payload)

def run(message_payload):
    """
    Main function to fetch, process, and save market data with batching.
    """
    log_message("Running fetch_market_index_data job...")
    log_message(f"Received message payload: {message_payload}")

    # Extract the list of symbols from message_payload
    if isinstance(message_payload, dict) and "symbols" in message_payload:
        symbols = message_payload["symbols"]
    else:
        log_message("Error: message_payload must be a dictionary with a 'symbols' key.")
        return
    
    log_message(f"Received symbols: {symbols}")

    if not isinstance(symbols, list):
        log_message("Error: symbols must be a list of strings.")
        return

    log_message("Starting fetch_market_index_data job...")

    # Check existing data
    existing_data = get_existing_market_index_data(symbols)
    if len(existing_data) == len(symbols):
        log_message("All market index data exists. Using existing data.")
        publish_market_index_data_update_complete(existing_data, total_symbols=len(symbols))
        return

    log_message("Some market index data is missing. Attempting to quote new data...")

    try:
        # Batching logic
        batch_size = 100
        all_results = []
        start_time = time.time()
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            log_message(f"Processing batch {i//batch_size+1} with {len(batch_symbols)} symbols: {batch_symbols}")
            
            # Quote market data for this batch
            log_message(f"Attempting to quote market data for batch {i//batch_size+1}...")
            raw_data = quote_market_index_data(batch_symbols)
            log_message(f"Successfully quoted market data for batch {i//batch_size+1}: {raw_data}")
            
            processed_data = process_market_data(raw_data)
            log_message(f"Processed market data for batch {i//batch_size+1}: {processed_data}")

            # Save data to the database
            save_market_data_to_db(processed_data)
            all_results.extend(processed_data)
            
            log_message(f"Completed batch {i//batch_size+1} with {len(processed_data)} records.")

        processing_time_ms = int((time.time() - start_time) * 1000)
        total_batches = (len(symbols) + batch_size - 1) // batch_size

        # Publish Kafka topic with batch metadata
        publish_market_index_data_update_complete(
            all_results, 
            total_batches=total_batches, 
            total_symbols=len(symbols), 
            processing_time_ms=processing_time_ms
        )
        log_message(f"Market data quoted and saved successfully. Processed {len(symbols)} symbols in {total_batches} batches in {processing_time_ms}ms.")
        
    except ValueError as e:
        log_message(f"Configuration error: {str(e)}")
        # If we have existing data, use it even if incomplete
        if existing_data:
            log_message("Using existing data despite configuration error.")
            publish_market_index_data_update_complete(existing_data, total_symbols=len(symbols))
        else:
            log_message("No existing data available and configuration error occurred.")
    except Exception as e:
        log_message(f"Error during market data processing: {str(e)}")
        log_message(f"Error details: {type(e).__name__}: {str(e)}")
        # If we have existing data, use it even if there was an error
        if existing_data:
            log_message("Using existing data despite processing error.")
            publish_market_index_data_update_complete(existing_data, total_symbols=len(symbols))
        else:
            log_message("No existing data available and processing error occurred.")

if __name__ == "__main__":
    run()