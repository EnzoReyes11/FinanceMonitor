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
ALPHA_VANTAGE_URL = os.environ.get("ALPHA_VANTAGE_URL")


def _get_symbol_latest(symbol):
    stock_values_response = None
    response = None

    if symbol is None or symbol == "":
        logging.warning("Alpha Vantage: Attempted to retrieve an empty symbol.")
        return None

    try:
        logging.info("Retrieving latest information for symbol %s", symbol)
        response = requests.get(
            ALPHA_VANTAGE_URL,
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

        metadata = stock_values_response.get("Meta Data")
        data_points = stock_values_response.get("Time Series (Daily)")

        latest_date = None
        latest_value = None

        if metadata and data_points:
            latest_date = metadata.get("3. Last Refreshed")
            if latest_date and latest_date in data_points:
                latest_value = data_points[latest_date]

        stock = None
        if latest_value:
            stock = {
                "symbol": symbol,
                "date": latest_date,
                "values": {
                    "open": latest_value.get("1. open"),
                    "high": latest_value.get("2. high"),
                    "low": latest_value.get("3. low"),
                    "close": latest_value.get("4. close"),
                    "volume": latest_value.get("5. volume"),
                },
            }

        logging.debug("Latest info %s", stock)
        logging.info("Successfully called symbol %s", symbol)
        return stock

    except KeyError:
        logging.exception("Error opening the Alpha Vantage response: %s", response)

        return None


def alpha_vantage_handler(request):
    if not ALPHA_VANTAGE_API_TOKEN or not ALPHA_VANTAGE_URL:
        logging.error(
            "FATAL: ALPHA_VANTAGE_API_TOKEN and ALPHA_VANTAGE_URL environment variables are not set."
        )
        return

    if request.method == "POST":
        request_data = request.get_json()
        logging.debug(request_data)

        for symbol in request_data.get("symbols", []):
            logging.info("Requested symbol: %s", symbol)
            symbol_data = _get_symbol_latest(symbol)

        return jsonify(symbol_data)

    if request.method == "GET":
        logging.info("Request : %s", request.args)
        symbol = request.args.get("symbol") or ""

        symbol_data = _get_symbol_latest(symbol)
        logging.info(symbol_data)

        return jsonify(symbol_data)
