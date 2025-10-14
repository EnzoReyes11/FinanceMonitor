import logging
import os
from datetime import datetime, timedelta
from http.client import HTTPConnection

import requests
from dotenv import load_dotenv

load_dotenv()


HTTPConnection.debuglevel = 1
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


# --- Configuration ---
IOL_USERNAME = os.environ.get("IOL_USERNAME")
IOL_PASSWORD = os.environ.get("IOL_PASSWORD")

TOKEN_URL = "https://api.invertironline.com/token"
FCI_LIST_URL = "https://api.invertironline.com/api/v2/Titulos/FCI"
LETRAS_URL = "https://api.invertironline.com/api/v2/Cotizaciones/letras/argentina/Todos?cotizacionInstrumentoModel.instrumento=letras&cotizacionInstrumentoModel.pais=argentina"
DAILY_QUOTES_BYMA = {
    'FCI': 'Titulos/FCI"',
    'LETRAS': 'Cotizaciones/letras/argentina/Todos?cotizacionInstrumentoModel.instrumento=letras&cotizacionInstrumentoModel.pais=argentina',
    'ON': 'Cotizaciones/obligacionesNegociables/argentina/Todos?cotizacionInstrumentoModel.instrumento=obligacionesNegociables&cotizacionInstrumentoModel.pais=argentina',
    'BONOS': 'Cotizaciones/titulosPublicos/argentina/Todos?cotizacionInstrumentoModel.instrumento=titulosPublicos&cotizacionInstrumentoModel.pais=argentina'
}

EXPIRE_BUFFER = 60

# --- Global variable to cache token in memory for the lifetime of the function instance ---
# This simple cache helps avoid re-authenticating on every warm invocation.
_cached_token_info = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": None,  # Stores the datetime when the token is considered expired
}


def _get_token_from_credentials(username, password):
    """
    Authenticates with username and password to get a new access and refresh token.
    Updates the global _cached_token_info.
    """
    payload = {"username": username, "password": password, "grant_type": "password"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logging.info(f"Attempting to get new token from {TOKEN_URL} for user.")
        logging.info("Username: %s", username)
        response = requests.post(TOKEN_URL, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()

        expires_in_seconds = token_data.get("expires_in", 0)
        # Add a buffer to consider the token expired a bit earlier
        expires_at = datetime.now() + timedelta(
            seconds=expires_in_seconds - EXPIRE_BUFFER
        )

        logging.info("Bearer %s", token_data["access_token"])

        _cached_token_info["access_token"] = token_data["access_token"]
        _cached_token_info["refresh_token"] = token_data["refresh_token"]
        _cached_token_info["expires_at"] = expires_at

        logging.info("Successfully obtained new token using credentials.")
        return True

    except requests.exceptions.RequestException as e:
        logging.exception("Error obtaining token with credentials:")

        if e.response is not None:
            logging.exception(
                f"Response status: {e.response.status_code}, content: {e.response.text}"
            )
        return False

    except KeyError:
        # Handle cases where the token response might be missing expected keys
        token_data_str = (
            str(token_data) if "token_data" in locals() else "Unknown structure"
        )
        logging.exception(
            f"Error parsing token response (missing key): {token_data_str}"
        )
        return False


def _refresh_access_token(current_refresh_token):
    """
    Refreshes an existing access token using a refresh token.
    Updates the global _cached_token_info.
    """
    payload = {"refresh_token": current_refresh_token, "grant_type": "refresh_token"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logging.info("Attempting to refresh token using refresh_token.")
        response = requests.post(TOKEN_URL, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()

        expires_in_seconds = token_data.get("expires_in", 0)
        expires_at = datetime.now() + timedelta(
            seconds=expires_in_seconds - EXPIRE_BUFFER
        )

        _cached_token_info["access_token"] = token_data["access_token"]
        # The API might not always return a new refresh token. Only update if provided.
        _cached_token_info["refresh_token"] = token_data.get(
            "refresh_token", current_refresh_token
        )
        _cached_token_info["expires_at"] = expires_at
        logging.info("Successfully refreshed token.")

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing token: {e}")

        if e.response is not None:
            logging.error(
                f"Response status: {e.response.status_code}, content: {e.response.text}"
            )
        return False

    except KeyError as e:
        token_data_str = (
            str(token_data) if "token_data" in locals() else "Unknown structure"
        )
        logging.error(
            f"Error parsing refreshed token response (missing key {e}): {token_data_str}"
        )
        return False


def get_valid_access_token():
    """
    Ensures a valid access token is available, fetching or refreshing if necessary.
    Returns the access token string or None if an error occurs.
    """
    # Check if cached token exists and is not expired (or close to expiry)
    if (
        _cached_token_info["access_token"]
        and _cached_token_info["expires_at"]
        and datetime.now() < _cached_token_info["expires_at"]
    ):
        logging.info("Using cached valid token.")

        return _cached_token_info["access_token"]

    # Try to refresh if a refresh token exists and current token is invalid/expired
    if _cached_token_info["refresh_token"]:
        logging.info(
            "Cached token is invalid or expired. Attempting to refresh token..."
        )

        if _refresh_access_token(_cached_token_info["refresh_token"]):
            return _cached_token_info["access_token"]
        else:
            # Refresh failed, clear stale refresh token to force full re-authentication
            logging.warning(
                "Refresh token failed. Clearing stale refresh token to force re-authentication."
            )

            _cached_token_info["refresh_token"] = None
            _cached_token_info["access_token"] = None
            _cached_token_info["expires_at"] = None

    # If no valid token or refresh failed, get a new one using credentials
    logging.info("Attempting to get new token using credentials...")
    if _get_token_from_credentials(IOL_USERNAME, IOL_PASSWORD):
        return _cached_token_info["access_token"]

    logging.error("Failed to obtain a valid access token after all attempts.")
    return None


def _make_authenticated_api_call(url):
    """
    Makes an authenticated API call, handling token acquisition and refresh.
    Retries once if the token is expired (401).

    Args:
        url (str): The API endpoint URL to call.

    Returns:
        The JSON response data or None if an error occurs.
    """
    # Attempt 1
    access_token = get_valid_access_token()
    if not access_token:
        logging.error("Failed to obtain access token for API call to %s", url)
        return None

    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        logging.info(f"Attempt 1: Calling API at {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # If the first attempt fails with a 401, our token is bad.
        # Invalidate it and try to get a new one.
        if e.response is not None and e.response.status_code == 401:
            logging.warning(
                "Received 401 from API for url %s. Invalidating token and retrying.", url
            )
            # Invalidate the token so get_valid_access_token is forced to refresh/re-auth
            _cached_token_info["access_token"] = None
            _cached_token_info["expires_at"] = None

            # Attempt 2 (retry)
            logging.info("Retrying token acquisition and API call.")
            access_token = get_valid_access_token()
            if not access_token:
                logging.error("Failed to obtain access token on retry for %s.", url)
                return None

            headers = {"Authorization": f"Bearer {access_token}"}
            try:
                logging.info(f"Attempt 2: Calling API at {url}")
                response_retry = requests.get(url, headers=headers)
                response_retry.raise_for_status()
                logging.info("Successfully called API on retry.")
                return response_retry.json()
            except requests.exceptions.RequestException as e_retry:
                logging.error(f"Error on retry API call to {url}: {e_retry}")
                if e_retry.response is not None:
                    logging.error(
                        f"Response status: {e_retry.response.status_code}, content: {e_retry.response.text}"
                    )
                return None
        else:
            # Handle other request exceptions (network errors, etc.)
            logging.error(f"Error calling API at {url}: {e}")
            if e.response is not None:
                logging.error(
                    f"Response status: {e.response.status_code}, content: {e.response.text}"
                )
            return None


def list_fci_data():
    """
    Calls the ListadoFCI API endpoint (/api/v2/Titulos/FCI).
    Returns the JSON response data or None if an error occurs.
    """
    logging.info(f"Requesting FCI data from {FCI_LIST_URL}")
    return _make_authenticated_api_call(FCI_LIST_URL)


def get_daily_quotes_data():
    """
    Calls various daily quotes API endpoints defined in DAILY_QUOTES_BYMA.
    Returns the JSON response data or None if an error occurs.
    """
    all_quotes_data = {}
    for category, endpoint_suffix in DAILY_QUOTES_BYMA.items():
        url = f"https://api.invertironline.com/api/v2/{endpoint_suffix}"
        all_quotes_data[category] = _make_authenticated_api_call(url)

    # Only return None if all calls failed, otherwise return partial data.
    if all(value is None for value in all_quotes_data.values()):
        logging.error("All daily quote API calls failed.")
        return None

    return all_quotes_data


def iol_api_handler(request):
    """
    Google Cloud Function HTTP trigger.
    This function will attempt to get an auth token and then call the FCI list API.
    The 'request' parameter is a Flask request object (not used in this example).
    """
    if not IOL_USERNAME or not IOL_PASSWORD:
        logging.error(
            "FATAL: IOL_USERNAME and IOL_PASSWORD environment variables are not set."
        )
        return (
            "Server configuration error: Missing credentials.",
            500,
            {"Content-Type": "text/plain"},
        )

    fci_data = list_fci_data()

    if fci_data is not None:
        from flask import jsonify

        return jsonify(fci_data)
    else:
        logging.error("Failed to retrieve FCI data after all attempts.")
        return (
            "Failed to retrieve data from target API.",
            500,
            {"Content-Type": "text/plain"},
        )
