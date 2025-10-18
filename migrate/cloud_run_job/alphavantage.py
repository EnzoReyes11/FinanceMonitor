"""Handles interaction with the Alpha Vantage API to retrieve stock data.

This module provides functions to fetch the latest daily stock prices for a given
symbol. It is designed to be used within a Flask application or a Google Cloud
Function environment.

It requires the following environment variables to be set:
- ALPHA_VANTAGE_API_TOKEN: Your API key for Alpha Vantage.
- ALPHA_VANTAGE_API_URL: The base URL for the Alpha Vantage API.
"""
import logging
import os

import requests
from dotenv import load_dotenv
from flask import jsonify

load_dotenv()


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


# --- Configuration ---
ALPHA_VANTAGE_API_TOKEN = os.environ.get("ALPHA_VANTAGE_API_TOKEN")
ALPHA_VANTAGE_API_URL = os.environ.get("ALPHA_VANTAGE_API_URL")


def _get_symbol_latest(symbol):
    """Fetches the latest daily data for a single stock symbol from Alpha Vantage.

    This function sends a request to the Alpha Vantage 'TIME_SERIES_DAILY'
    endpoint, parses the response, and extracts the data for the most recent
    day available.

    Args:
        symbol (str): The stock ticker symbol (e.g., "AAPL").

    Returns:
        dict: A dictionary containing the latest stock information, including
              'ticker', 'price', 'market', and 'date'.
              Returns None if the symbol is invalid, the API call fails,
              or the response format is unexpected.
    """
    stock_values_response = None
    response = None

    if symbol is None or symbol == "":
        logging.warning("Alpha Vantage: Attempted to retrieve an empty symbol.")
        return None

    try:
        logging.info("Retrieving latest information for symbol %s", symbol)
        response = requests.get(
            ALPHA_VANTAGE_API_URL,
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "apikey": ALPHA_VANTAGE_API_TOKEN,
            },
        )

    except requests.exceptions.RequestException:
        logging.exception("Error calling Alpha Vantage API")

        if response is not None:
            logging.error(f"Response status: content: {response}")

        return None

    try:
        stock_values_response = response.json()

        # Extract main sections from the API response.
        metadata = stock_values_response.get("Meta Data")
        data_points = stock_values_response.get("Time Series (Daily)")

        latest_date = None
        latest_value = None

        if metadata and data_points:
            # The '3. Last Refreshed' field in metadata gives us the key for the latest data point.
            latest_date = metadata.get("3. Last Refreshed")
            if latest_date and latest_date in data_points:
                latest_value = data_points[latest_date]
        else:
            # This can happen for invalid symbols or API rate limiting.
            logging.error("Alpha Vantage response missing expected values")
            logging.error(f"Response: {stock_values_response}")

            return None

        stock = None
        if latest_value:
            # Construct the final object with the required fields.
            stock = {
                "ticker": symbol,
                "price": latest_value.get("4. close"),
                "market": "US",
                "date": latest_date,
                # Other possible values:
                #                "values": {
                #                    "open": latest_value.get("1. open"),
                #                    "high": latest_value.get("2. high"),
                #                    "low": latest_value.get("3. low"),
                #                    "close": latest_value.get("4. close"),
                #                    "volume": latest_value.get("5. volume"),
                #                },
            }

        logging.debug("Latest info %s", stock)
        logging.info("Successfully called symbol %s", symbol)
        return stock

    except KeyError:
        logging.exception("Error opening the Alpha Vantage response: %s", response)

        return None


def alpha_vantage_handler(request):
    """HTTP request handler for fetching Alpha Vantage data.

    This function acts as an endpoint for a Flask application or Cloud Function.
    It supports both GET and POST methods to retrieve stock data.

    - GET: Expects a 'symbol' query parameter (e.g., /?symbol=AAPL).
    - POST: Expects a JSON body with a 'symbols' list (e.g., {"symbols": ["AAPL", "MSFT"]}).
      Note: Currently, it only processes and returns data for the *last* symbol in the list.

    Args:
        request (flask.Request): The incoming HTTP request object.

    Returns:
        A Flask Response object containing JSON data. On success, it returns
        the stock data. On failure, it returns a JSON error message with an
        appropriate HTTP status code. Returns None if environment variables
        are not set.
    """
    if not ALPHA_VANTAGE_API_TOKEN or not ALPHA_VANTAGE_API_URL:
        logging.error(
            "FATAL: ALPHA_VANTAGE_API_TOKEN and ALPHA_VANTAGE_URL environment variables are not set."
        )
        return

    if request.method == "POST":
        request_data = request.get_json()
        logging.debug(request_data)

        # Note: This loop will overwrite `symbol_data` on each iteration.
        # The final response will only contain data for the last symbol in the list.
        # TODO: Handle multiple symbols properly.
        for symbol in request_data.get("symbols", []):
            logging.info("Requested symbol: %s", symbol)
            symbol_data = _get_symbol_latest(symbol)

        if symbol_data is None:
            return jsonify({"error": "Internal Server Error"}), 500

        return jsonify(symbol_data)

    if request.method == "GET":
        logging.info("Request : %s", request.args)
        symbol = request.args.get("symbol") or ""

        symbol_data = _get_symbol_latest(symbol)

        if symbol_data is None:
            return jsonify({"error": "Internal Server Error"}), 500

        return jsonify(symbol_data)
