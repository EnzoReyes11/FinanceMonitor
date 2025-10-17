from typing import Any
from unittest.mock import MagicMock

import pytest

# Make sure the client is importable from your project structure
from alphavantage.client import AlphaVantageClient

# A realistic successful response from the Alpha Vantage API
MOCK_SUCCESS_RESPONSE:  dict[str, Any] = {
    "Meta Data": {
        "1. Information": "Daily Prices (open, high, low, close) and Volumes",
        "2. Symbol": "AAPL",
        "3. Last Refreshed": "2024-10-25",
        "4. Output Size": "Compact",
        "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
        "2024-10-25": {
            "1. open": "150.00",
            "2. high": "152.00",
            "3. low": "149.50",
            "4. close": "151.75",
            "5. volume": "12345678"
        },
        "2024-10-24": {
            "1. open": "148.00",
            # ... other data
        }
    }
}

# A realistic response for an invalid symbol
MOCK_INVALID_SYMBOL_RESPONSE = {
    "Error Message": "Invalid API call. Please check your API key and symbols."
}


# --- Pytest Fixtures ---

@pytest.fixture
def mock_logger() -> MagicMock:
    """Create a mock logger to inspect log messages."""
    return MagicMock()

@pytest.fixture
def client(mock_logger: MagicMock):
    """Create an instance of the AlphaVantageClient with a fake token."""
    return AlphaVantageClient(api_token="FAKE_API_KEY", logger=mock_logger)


# --- Test Cases ---

def test_get_latest_daily_success(client: AlphaVantageClient, requests_mock):
    """
    Test the happy path: a successful API call returns correctly parsed data.
    """
    # Arrange: Mock the API to return a successful response
    requests_mock.get(client.api_url, json=MOCK_SUCCESS_RESPONSE, status_code=200)

    # Act: Call the method under test
    result = client.get_latest_daily("AAPL")

    # Assert: Check that the result is correctly parsed
    assert result is not None
    assert result["ticker"] == "AAPL"
    assert result["price"] == 151.75
    assert result["date"] == "2024-10-25"
    assert result["market"] == "US"
    client.logger.info.assert_called_with("Successfully retrieved latest data for symbol AAPL")

def test_get_latest_daily_invalid_symbol(client, requests_mock):
    """
    Test that the client handles an invalid symbol response gracefully.
    """
    # Arrange: Mock the API to return an error message
    requests_mock.get(client.api_url, json=MOCK_INVALID_SYMBOL_RESPONSE, status_code=200)

    result = client.get_latest_daily("INVALID")

    # Assert: Check that the method returns None and logs an error
    assert result is None
    client.logger.error.assert_called_with("Alpha Vantage response for INVALID is missing expected data.")

def test_get_latest_daily_http_error(client, requests_mock):
    """
    Test that the client handles a network or server-side error (e.g., 500).
    """
    requests_mock.get(client.api_url, status_code=500, text="Internal Server Error")

    result = client.get_latest_daily("AAPL")

    # Assert: Check that the method returns None and logs the exception
    assert result is None
    client.logger.exception.assert_called()

def test_get_latest_daily_malformed_json(client, requests_mock):
    """
    Test that the client handles a response with invalid JSON.
    """
    # Arrange: Mock a response with non-JSON content
    requests_mock.get(client.api_url, status_code=200, text="<HTML>This is not JSON</HTML>")

    result = client.get_latest_daily("AAPL")

    # Assert: Check that the method returns None and logs the decoding error
    assert result is None
    client.logger.exception.assert_called_with("Error decoding JSON response for symbol AAPL.")

def test_get_latest_daily_empty_symbol(client):
    """
    Test that the client rejects an empty or None symbol without making an API call.
    """
    result = client.get_latest_daily("")

    # Assert: Check that the method returns None and logs a warning
    assert result is None
    client.logger.warning.assert_called_with("Attempted to retrieve an empty or null symbol.")

def test_client_initialization_requires_token():
    """
    Test that the client cannot be initialized without an API token.
    """
    # Act & Assert: Use pytest.raises to confirm a ValueError is thrown
    with pytest.raises(ValueError, match="API token cannot be empty."):
        AlphaVantageClient(api_token="")

    with pytest.raises(ValueError, match="API token cannot be empty."):
        AlphaVantageClient(api_token=None)