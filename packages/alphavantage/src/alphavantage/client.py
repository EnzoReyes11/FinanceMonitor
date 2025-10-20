import logging
from typing import Any, Dict, Optional

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class AlphaVantageClient:
    """
    A client for interacting with the Alpha Vantage API to retrieve stock data.

    This class encapsulates the logic for making API requests and parsing
    the responses in a reusable and testable way.
    """

    def __init__(
        self,
        api_token: str,
        api_url: str = "https://www.alphavantage.co/query",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initializes the AlphaVantageClient.

        Args:
            api_token (str): Your API key for Alpha Vantage.
            api_url (str, optional): The base URL for the Alpha Vantage API.
                                     Defaults to "https://www.alphavantage.co/query".
            logger (Optional[logging.Logger], optional): An optional logger instance.
                                                         If None, a default logger is used.
        """
        if not api_token:
            raise ValueError("API token cannot be empty.")

        self.api_token = api_token
        self.api_url = api_url
        self.logger = logger or logging.getLogger(__name__)


    def get_short_backfill(self, symbol: str):
        if not symbol:
            self.logger.warning("Attempted to retrieve an empty or null symbol.")
            return None

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_token,
            "outputsize": "compact",
            "datatype": "csv"
        }

        try:
            self.logger.info(f"Retrieving latest information for symbol: {symbol}")
            response = requests.get(self.api_url, params=params, timeout=10)
            response.raise_for_status()
            stock_data = response.text
            return stock_data


        except ValueError: 
            self.logger.exception(f"Error decoding JSON response for symbol {symbol}.")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.exception(f"HTTP error occurred, symbol {symbol}:")
            self.logger.info(f"Request method: {e.request.method}")
            self.logger.info(f"Request URL: {e.request.url}")
            self.logger.info(f"Request headers: {e.request.headers}")
            self.logger.info(f"Request body: {e.request.body}")

            return None
        except requests.exceptions.Timeout as e:
            self.logger.exception(f"Timeout error on AlphaVantage call {symbol}.")
            self.logger.info(f"Request method: {e.request.method}")
            self.logger.info(f"Request URL: {e.request.url}")
            self.logger.info(f"Request headers: {e.request.headers}")
            self.logger.info(f"Request body: {e.request.body}")

            return None
        except requests.exceptions.RequestException:
            self.logger.exception(f"Error calling Alpha Vantage API for symbol {symbol}")

            return None




    def get_latest_daily(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the latest daily data for a single stock symbol.

        Args:
            symbol (str): The stock ticker symbol (e.g., "AAPL").

        Returns:
            Optional[Dict[str, Any]]: A dictionary with the latest stock info
                                      or None if the API call fails or the
                                      symbol is invalid.
        """
        if not symbol:
            self.logger.warning("Attempted to retrieve an empty or null symbol.")
            return None

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_token,
            "outputsize": "compact"
        }

        try:
            self.logger.info(f"Retrieving latest information for symbol: {symbol}")
            response = requests.get(self.api_url, params=params, timeout=10)
            response.raise_for_status()
            stock_data = response.json()
            return self._parse_daily_response(stock_data, symbol)

        except ValueError: 
            self.logger.exception(f"Error decoding JSON response for symbol {symbol}.")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.exception(f"Error calling Alpha Vantage API for symbol {symbol}: {e}")
            return None


    def _parse_daily_response(self, response_data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """
        Parses the JSON response from the 'TIME_SERIES_DAILY' endpoint.

        Args:
            response_data (Dict[str, Any]): The parsed JSON from the API response.
            symbol (str): The stock symbol requested, for inclusion in the output.

        Returns:
            Optional[Dict[str, Any]]: A formatted dictionary with the latest stock
                                      data, or None if the response is invalid.
        """
        metadata = response_data.get("Meta Data")
        data_points = response_data.get("Time Series (Daily)")

        if not metadata or not data_points:
            self.logger.error(f"Alpha Vantage response for {symbol} is missing expected data.")
            self.logger.debug(f"Invalid response: {response_data}")
            return None

        latest_date_str = metadata.get("3. Last Refreshed")
        latest_values = data_points.get(latest_date_str)

        if not latest_date_str or not latest_values:
            self.logger.warning(f"Could not find latest data for {symbol} in the response.")
            return None

        try:
            stock = {
                "ticker": symbol,
                "price": float(latest_values.get("4. close")),
                "market": "US",
                "date": latest_date_str,
            }
            self.logger.info(f"Successfully retrieved latest data for symbol {symbol}")
            return stock
        except (TypeError, ValueError) as e:
            self.logger.exception(f"Error parsing latest value fields for {symbol}: {e}")
            return None

