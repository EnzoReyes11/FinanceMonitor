import logging
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Configuration ---
IOL_USERNAME = os.environ.get("IOL_USERNAME")
IOL_PASSWORD = os.environ.get("IOL_PASSWORD")

TOKEN_URL = "https://api.invertironline.com/token"
FCI_LIST_URL = "https://api.invertironline.com/api/v2/Titulos/FCI"

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
        logging.error(f"Error obtaining token with credentials: {e}")
        if e.response is not None:
            logging.error(
                f"Response status: {e.response.status_code}, content: {e.response.text}"
            )
        return False
    except KeyError as e:
        # Handle cases where the token response might be missing expected keys
        token_data_str = (
            str(token_data) if "token_data" in locals() else "Unknown structure"
        )
        logging.error(
            f"Error parsing token response (missing key {e}): {token_data_str}"
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


def list_fci_data(access_token):
    """
    Calls the ListadoFCI API endpoint (/api/v2/Titulos/FCI).
    Returns the JSON response data or None if an error occurs.
    """
    if not access_token:
        logging.error("No access token provided for list_fci_data.")
        return None

    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        logging.info(f"Calling ListadoFCI API at {FCI_LIST_URL}")
        response = requests.get(FCI_LIST_URL, headers=headers)
        response.raise_for_status()
        logging.info("Successfully called ListadoFCI API.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error calling ListadoFCI API: {e}")

        if e.response is not None:
            logging.error(
                f"Response status: {e.response.status_code}, content: {e.response.text}"
            )

            if e.response.status_code == 401:
                # Token likely expired. Invalidate the cached token so the next
                # call to get_valid_access_token re-authenticates or refreshes.
                logging.warning(
                    "Received 401 from API, invalidating cached access token."
                )
                _cached_token_info["access_token"] = None
                _cached_token_info["expires_at"] = None  # Mark as expired

        return None


# --- Google Cloud Function entry point ---
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

    # Attempt 1 to get data
    access_token = get_valid_access_token()
    if not access_token:
        logging.error("Failed to obtain access token on initial attempt.")
        return ("Authentication failed.", 500, {"Content-Type": "text/plain"})

    fci_data = list_fci_data(access_token)

    if fci_data is not None:
        # Using requests.Response for proper JSON handling by GCF
        from flask import jsonify

        return jsonify(
            fci_data
        )  # Returns a Flask Response object with application/json
    else:
        # This 'else' means list_fci_data returned None.
        # This could be due to a 401 (token expired just before API call) or other API error.
        # If it was 401, list_fci_data would have invalidated the token.
        logging.warning(
            "First attempt to list FCI data failed. Retrying token acquisition and API call..."
        )

        # Attempt 2 (retry after potential token invalidation)
        access_token = (
            get_valid_access_token()
        )  # This will re-auth or refresh if token was invalidated
        if not access_token:
            logging.error("Failed to obtain access token on retry.")
            return (
                "Authentication failed on retry.",
                500,
                {"Content-Type": "text/plain"},
            )

        fci_data = list_fci_data(access_token)
        if fci_data is not None:
            from flask import jsonify

            return jsonify(fci_data)
        else:
            logging.error("Failed to retrieve FCI data after retry.")
            return (
                "Failed to retrieve data from target API after retry.",
                500,
                {"Content-Type": "text/plain"},
            )
