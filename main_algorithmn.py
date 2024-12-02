from etf_scraper import ETFScraper
import pandas as pd
import numpy as np
from polygon import RESTClient
from datetime import datetime, timedelta
import csv
import time

etf_scraper = ETFScraper()

SPY_ticker = "SPY" # IShares Core S&P 500 ETF

QQQ_ticker = "QQQ" # IShares Core QQQ ETF


QQQ_df = etf_scraper.query_holdings(QQQ_ticker)
QQQ_list = QQQ_df['ticker'].tolist()

SPY_df = etf_scraper.query_holdings(SPY_ticker)
SPY_list = SPY_df['ticker'].tolist()

all_stocks = set(SPY_list + QQQ_list)

# Save to a text file
with open('all_stocks.txt', 'w') as file:
    for stock in all_stocks:
        file.write(stock + '\n')

print("All stocks saved to all_stocks.txt")


def fetch_and_save_1min_data(client, ticker, start_date, end_date, output_folder="."):
    """
    Fetches 1-minute aggregate data for a given stock and saves it to a CSV file.

    Args:
        client (RESTClient): The Polygon.io REST client.
        ticker (str): The stock ticker symbol.
        start_date (datetime.date): The start date for fetching data.
        end_date (datetime.date): The end date for fetching data.
        output_folder (str): The folder where the CSV file will be saved. Default is current folder.
    """
    # Initialize a list to hold all aggregate data
    all_aggs = []
    current_start_date = start_date
    calls_made = 0

    print(f"Fetching data for {ticker}...")

    # Loop through each date range (monthly intervals)
    while current_start_date < end_date:
        # Set the current end date to one month ahead, but not beyond the overall end date
        current_end_date = min(current_start_date + timedelta(days=30), end_date)

        # Fetch data in 1-minute intervals for the current date range
        try:
            aggs = client.list_aggs(
                ticker,
                1,  # 1-minute interval
                "minute",
                current_start_date.strftime('%Y-%m-%d'),
                current_end_date.strftime('%Y-%m-%d'),
                limit=50000,  # Polygon's limit per request
            )

            # Append data to the all_aggs list
            for agg in aggs:
                all_aggs.append({
                    'timestamp': agg.timestamp,
                    'open': agg.open,
                    'high': agg.high,
                    'low': agg.low,
                    'close': agg.close,
                    'volume': agg.volume
                })
        except Exception as e:
            print(f"Error fetching data for {ticker} from {current_start_date} to {current_end_date}: {e}")

        # Move to the next date range
        current_start_date = current_end_date
        calls_made += 1

        # Enforce API rate limit
        if calls_made >= 5:
            print(f"Reached rate limit for {ticker}. Sleeping for 60 seconds...")
            time.sleep(60)  # Sleep for 1 minute
            calls_made = 0

    # Save the data to a CSV file
    csv_file = f"{output_folder}/{ticker}_1min_data.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        writer.writeheader()
        writer.writerows(all_aggs)

    print(f"Data for {ticker} saved to {csv_file} \n")


# Replace 'YOUR_API_KEY' with your actual Polygon.io API key
api_key = 'F1qpnmBGvKd9VCubtJhF3fVxhVLd0W3n'
client = RESTClient(api_key)

# Define the date range and list of QQQ stocks
end_date = datetime.now().date()
start_date = end_date - timedelta(days=2*365)  # Two years

ticker_index = 1

# Fetch and save data for each stock
for ticker in all_stocks:
    print(f"{ticker_index}: {ticker}")
    fetch_and_save_1min_data(client, ticker, start_date, end_date, output_folder="/stock_data")
    ticker_index += 1
