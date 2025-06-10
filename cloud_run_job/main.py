# /home/enzo/Projects/FinanceMonitor/main.py
import logging
import os

from dotenv import load_dotenv
from flask import Flask, request

# Load environment variables from .env file for local development
load_dotenv()

from alphavantage import alpha_vantage_handler  # noqa: E402
from bq import bq_batch_load_handler  # noqa: E402
from iol import iol_api_handler  # noqa: E402

# Configure logging. Cloud Run will capture stdout/stderr.
# Set log level from environment variable, defaulting to INFO.
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def start():
    logger.info("Root path '/' was called.")
    return "Finance Monitor API is running!"


@app.route("/iol", methods=["GET", "POST"])
def route_iol_data():
    logger.info("Received request for /iol endpoint.")

    return iol_api_handler(request)


@app.route("/alpha-vantage", methods=["GET", "POST"])
def route_alpha_vantage_data():
    logger.info("Received request for /alpha-vantage endpoint.")

    return alpha_vantage_handler(request)


@app.route("/bq-batch-load", methods=["POST"])
def route_bq_batch_load():
    logger.info("Received request for /bq-batch-load endpoint.")

    return bq_batch_load_handler(request)


if __name__ == "__main__":
    # This block is for local Flask development server.
    # Cloud Run uses the Gunicorn command specified in the Dockerfile.
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting Flask development server on http://0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, debug=True)
