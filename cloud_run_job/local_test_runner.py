# /home/enzo/Projects/FinanceMonitor/local_test_runner.py
import logging
import os

from alphavantage import alpha_vantage_handler
from bq import bq_batch_load_handler
from flask import Flask, request
from iol import iol_api_handler

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_iol_tests():
    """Runs local tests for the IOL API handler."""
    current_test_username = os.getenv("IOL_USERNAME")
    current_test_password = os.getenv("IOL_PASSWORD")

    if (
        current_test_username == "your_actual_username"
        or current_test_password == "your_actual_password"
    ):
        logging.warning(
            "Using placeholder credentials. Please update 'your_actual_username' and "
            "'your_actual_password' in local_test_runner.py or set real credentials "
            "in your environment / .env file."
        )
        # Consider returning here if actual tests against the API are desired
        # return
    else:
        logging.info("--- Starting local test of iol_api_handler ---")

    # iol_api_handler uses Flask's jsonify, so Flask needs to be available.
    # In a GCF environment, Flask is provided. For local testing, install it.
    try:
        from flask import Flask, request  # Required for app context if jsonify is used

        app = Flask(__name__)
        # Create an application context for jsonify to work correctly
        with app.test_request_context("/"):
            response_from_handler = iol_api_handler(request)

        if isinstance(response_from_handler, tuple):
            # This typically indicates an error before jsonify was called,
            # e.g., credential check failure or initial token acquisition failure.
            message, status_code, _ = response_from_handler
            logging.info(f"Handler returned tuple. Status Code: {status_code}")
            logging.error(f"Error Message: {message}")
        else:
            # This is expected to be a Flask Response object
            status_code = response_from_handler.status_code
            response_data_text = response_from_handler.get_data(as_text=True)
            logging.info(f"Handler returned Flask Response. Status Code: {status_code}")

            if status_code == 200:
                logging.info("FCI Data (first 500 chars):")
                print(response_data_text[:500] + "...")
            else:
                logging.error(f"Error Message: {response_data_text}")

    except ImportError:
        logging.error(
            "Flask not installed. iol_api_handler requires Flask for jsonify. "
            "Please install Flask for local testing (e.g., 'pip install Flask')."
        )
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during testing: {e}", exc_info=True
        )


def run_alpha_vantage_tests():
    try:
        app = Flask(__name__)

        with app.test_request_context(
            "/alpha-vantage", method="POST", json={"symbols": ["AAPL", "MSFT"]}
        ):
            logging.info('Testing POST request to "/alpha-vantage"')
            alpha_vantage_handler(request)

        with app.test_request_context("/alpha-vantage", method="GET"):
            logging.info('Testing GET request to "/alpha-vantage"')
            alpha_vantage_handler(request)
    except Exception:
        logging.exception("An unexpected error occurred during testing.")


def run_bq_batch_load():
    try:
        app = Flask(__name__)

        with app.test_request_context(
            "/bq-batch-load",
            method="POST",
            json={"symbols": [["AAPL", 42, "US", "2025-06-10"]]},
        ):
            logging.info('Testing POST request to "/bq-batch-load"')
            bq_batch_load_handler(request)

        with app.test_request_context("/bq-batch-load", method="GET"):
            logging.info('Testing GET request to "/bq-batch-load"')
            bq_batch_load_handler(request)
    except Exception:
        logging.exception("An unexpected error occurred during testing.")


if __name__ == "__main__":
    # run_alpha_vantage_tests()
    run_iol_tests()
    # run_bq_batch_load()
