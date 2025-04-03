# ETL Pipeline for FinTrack

The ETL (Extract, Transform, Load) pipeline is a crucial component of the FinTrack project, responsible for fetching, processing, and storing financial data from various sources. This document provides an overview of the ETL pipeline's functionality and usage.

## Overview

The ETL pipeline consists of three main scripts:

1. **fetch_data.py**: This script is responsible for extracting live market data from financial APIs. It connects to the specified APIs, retrieves the necessary data, and prepares it for processing.

2. **process_data.py**: After fetching the data, this script processes it to ensure it is in the correct format for storage. This may include data cleaning, transformation, and validation steps.

3. **store_data.py**: The final script in the pipeline stores the processed data into the PostgreSQL database. It handles the database connection and executes the necessary SQL commands to insert the data.

## Usage

To run the ETL pipeline, follow these steps:

1. Ensure that the PostgreSQL database is set up and running.
2. Update the configuration settings in the scripts as needed (e.g., API keys, database connection details).
3. Execute the scripts in the following order:
   - Run `fetch_data.py` to extract the data.
   - Run `process_data.py` to transform the data.
   - Run `store_data.py` to load the data into the database.

## Docker

The ETL pipeline can be containerized using Docker. The provided Dockerfile in this directory can be used to build a Docker image for the ETL pipeline, allowing for consistent deployment and execution across different environments.

## Conclusion

The ETL pipeline is essential for ensuring that the FinTrack application has access to up-to-date financial data. By automating the data extraction, transformation, and loading processes, the pipeline helps users make informed investment decisions based on real-time information.