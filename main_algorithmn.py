from etf_scraper import ETFScraper
from polygon import RESTClient
from datetime import datetime, timedelta
import pandas as pd
import csv
import time
import os

# --- Initialization ---
# Replace 'YOUR_API_KEY' with your actual Polygon.io API key
API_KEY = 'F1qpnmBGvKd9VCubtJhF3fVxhVLd0W3n'
client = RESTClient(API_KEY)

# Define ETF tickers
SPY_TICKER = "SPY"  # IShares Core S&P 500 ETF
QQQ_TICKER = "QQQ"  # IShares Core QQQ ETF

# Define the date range for fetching data
END_DATE = datetime(2024, 12, 3)  # Set the desired end date
START_DATE = END_DATE - timedelta(days=2 * 365)  # Two years of data

# Output folder for stock data
OUTPUT_FOLDER = "./stock_data"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)  # Ensure the output folder exists

# --- Helper Functions ---
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
    all_aggs = []  # Holds all aggregate data
    current_start_date = start_date
    calls_made = 0

    print(f"Fetching data for {ticker}...")

    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=30), end_date)

        try:
            aggs = client.list_aggs(
                ticker,
                1,  # 1-minute interval
                "minute",
                current_start_date.strftime('%Y-%m-%d'),
                current_end_date.strftime('%Y-%m-%d'),
                limit=50000,  # Polygon's limit per request
            )

            all_aggs.extend({
                'timestamp': agg.timestamp,
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume
            } for agg in aggs)

        except Exception as e:
            print(f"Error fetching data for {ticker} ({current_start_date} to {current_end_date}): {e}")

        current_start_date = current_end_date
        calls_made += 1

        # Enforce API rate limits
        if calls_made >= 5:
            print(f"Rate limit reached for {ticker}. Sleeping for 60 seconds...")
            time.sleep(60)
            calls_made = 0

    # Save data to CSV
    csv_file = os.path.join(output_folder, f"{ticker}_1min_data.csv")
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        writer.writeheader()
        writer.writerows(all_aggs)

    print(f"Data for {ticker} saved to {csv_file}\n")

# --- Main Execution ---
def main():
    # Initialize ETF scraper and get stock holdings
    etf_scraper = ETFScraper()

    QQQ_df = etf_scraper.query_holdings(QQQ_TICKER)
    SPY_df = etf_scraper.query_holdings(SPY_TICKER)

    QQQ_list = QQQ_df['ticker'].tolist()
    SPY_list = SPY_df['ticker'].tolist()

    # Combine holdings and remove duplicates
    all_stocks = set(QQQ_list + SPY_list)

    # Save all stock tickers to a file
    with open('all_stocks.txt', 'w') as file:
        for stock in sorted(all_stocks):
            file.write(stock + '\n')
    print("All stocks saved to all_stocks.txt")

    # Fetch and save 1-minute data for each stock
    for index, ticker in enumerate(all_stocks, start=1):
        print(f"{index}: {ticker}")
        fetch_and_save_1min_data(client, ticker, START_DATE, END_DATE, output_folder=OUTPUT_FOLDER)

if __name__ == "__main__":
    main()
