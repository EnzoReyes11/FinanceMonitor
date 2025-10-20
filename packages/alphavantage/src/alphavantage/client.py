import io
import logging
from typing import Optional

import pandas as pd
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


    def get_short_backfill(self, symbol: str) -> pd.DataFrame:
        if not symbol:
            self.logger.warning("Attempted to retrieve an empty or null symbol.")
            return pd.DataFrame() 

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

            return pd.read_csv(io.StringIO(response.text))

        except ValueError: 
            self.logger.exception(f"Error decoding JSON response for symbol {symbol}.")

            return pd.DataFrame() 
        except requests.exceptions.HTTPError as e:
            self.logger.exception(f"HTTP error occurred, symbol {symbol}:")
            self.logger.info(f"Request method: {e.request.method}")
            self.logger.info(f"Request URL: {e.request.url}")
            self.logger.info(f"Request headers: {e.request.headers}")
            self.logger.info(f"Request body: {e.request.body}")

            return pd.DataFrame() 
        except requests.exceptions.Timeout as e:
            self.logger.exception(f"Timeout error on AlphaVantage call {symbol}.")
            self.logger.info(f"Request method: {e.request.method}")
            self.logger.info(f"Request URL: {e.request.url}")
            self.logger.info(f"Request headers: {e.request.headers}")
            self.logger.info(f"Request body: {e.request.body}")

            return pd.DataFrame() 
        except requests.exceptions.RequestException:
            self.logger.exception(f"Error calling Alpha Vantage API for symbol {symbol}")

            return pd.DataFrame() 
