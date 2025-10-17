import os

from alphavantage.client import AlphaVantageClient

if __name__ == '__main__':
    api_key = os.environ.get("ALPHA_VANTAGE_API_TOKEN", "YOUR_API_KEY")

    client = AlphaVantageClient(api_token=api_key)

   # apple_stock = client.get_latest_daily("AAPL")
   # if apple_stock:
   #     print(f"Successfully fetched Apple stock data: {apple_stock}")

   # invalid_stock = client.get_latest_daily("INVALID_SYMBOL")
   # if not invalid_stock:
   #     print("Correctly handled an invalid symbol.")