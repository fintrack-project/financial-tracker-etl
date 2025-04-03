import requests
import json
import os

def fetch_market_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {api_url}: {e}")
        return None

def save_data_to_file(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def main():
    # Example API URL for fetching market data
    api_url = "https://api.example.com/marketdata"
    
    # Fetch market data
    market_data = fetch_market_data(api_url)
    
    if market_data:
        # Define the output file path
        output_file = os.path.join(os.path.dirname(__file__), 'market_data.json')
        
        # Save the fetched data to a file
        save_data_to_file(market_data, output_file)
        print(f"Market data saved to {output_file}")

if __name__ == "__main__":
    main()