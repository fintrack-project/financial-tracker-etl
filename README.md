# Financial Tracker ETL Pipeline

The Financial Tracker ETL (Extract, Transform, Load) pipeline is a robust system designed to automate the ingestion, processing, and storage of financial data. This pipeline supports various financial data sources and ensures the data is ready for analysis and reporting.

## **Overview**

The ETL pipeline is a critical component of the Financial Tracker project, enabling seamless data integration and management. It consists of multiple jobs and utilities that handle the following tasks:

1. **Data Extraction**:
   - Fetches live market data from APIs.
   - Retrieves historical data for financial assets.

2. **Data Transformation**:
   - Cleans and validates raw data.
   - Transforms data into a structured format for storage.

3. **Data Loading**:
   - Stores processed data into a PostgreSQL database.
   - Ensures data consistency and integrity with relational constraints.

## **Key Components**

### **1. ETL Jobs**
The project includes several ETL jobs, each responsible for a specific part of the pipeline:

- **`fetch_historical_market_data`**:
  - Fetches historical market data for financial assets (e.g., stocks, forex, crypto).
  - Identifies missing data ranges and updates the database accordingly.

- **`fetch_market_data`**:
  - Fetches real-time market data for assets.
  - Updates the `market_data` table with current prices and changes.

- **`fetch_market_index_data`**:
  - Fetches market index data for symbols.
  - Updates the `market_data` table with index information.

- **`generate_mock_transactions`**:
  - Generates mock transaction data for testing purposes.
  - Supports cash and stock transactions with customizable parameters.

### **2. Utilities**
The project includes reusable utilities to support ETL operations:

- **Database Utilities**:
  - Handles database connections and query execution.
  - Ensures efficient interaction with the PostgreSQL database.

- **Logging**:
  - Provides centralized logging for debugging and monitoring.

- **Kafka Integration**:
  - Publishes messages to Kafka topics to signal job completion or trigger downstream processes.

### **3. Database Schema**
The pipeline interacts with the following key tables:

- **`transactions`**:
  - Stores user transactions, including credits and debits for assets.

- **`holdings`**:
  - Tracks the current balance of assets for each account.
  - Updated synchronously by the backend when transactions are confirmed.

- **`holdings_monthly`**:
  - Tracks monthly balances for assets, enabling historical analysis.
  - Updated synchronously by the backend when transactions are confirmed.

- **`asset`**:
  - Stores metadata about financial assets, such as symbols, units, and types.

- **`market_data`**:
  - Stores live and historical market data for financial assets.

## **Usage**

### **1. Prerequisites**
- **Python 3.x**: Ensure Python is installed on your system.
- **PostgreSQL**: Set up a PostgreSQL database for storing financial data.
- **Docker (Optional)**: Use Docker for containerized deployment.

### **2. Running the ETL Pipeline**
To execute the ETL pipeline, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/financial-tracker-etl.git
   cd financial-tracker-etl

2. Install dependencies:
   pip install -r requirements.txt

3. Configure environment variables:
   - Set up database connection details (e.g., DB_HOST, DB_USER, DB_PASSWORD).
   - Add API keys for financial data sources if required.

4. Run individual ETL jobs:

- Fetch historical market data:
python etl/jobs/fetch_historical_market_data/fetch_historical_market_data.py

- Fetch real-time market data:
python etl/jobs/fetch_market_data/fetch_market_data.py

- Fetch market index data:
python etl/jobs/fetch_market_index_data/fetch_market_index_data.py

### **3. Docker Deployment**
The ETL pipeline can be containerized using Docker for consistent deployment across environments. Use the provided Dockerfile to build and run the pipeline:

docker build -t financial-tracker-etl .
docker run -e DB_HOST=<db_host> -e DB_USER=<db_user> -e DB_PASSWORD=<db_password> financial-tracker-etl

## **Project Structure**

financial-tracker-etl/
├── etl/
│   ├── jobs/
│   │   ├── fetch_historical_market_data/
│   │   ├── fetch_market_data/
│   │   ├── fetch_market_index_data/
│   │   ├── generate_mock_transactions.py
│   ├── utils/
│   │   ├── db_utils.py
│   │   ├── log_utils.py
│   │   ├── fetch_utils.py
├── Dockerfile
├── requirements.txt
├── README.md

## **License**

This project is licensed under the MIT License. See the LICENSE file for details.