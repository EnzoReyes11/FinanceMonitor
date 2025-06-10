import csv
import io
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import jsonify
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def bq_load_data(symbols_data):
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
    BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

    if not all([BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
        logger.error(
            "Missing environment variables (BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)."
        )

        return jsonify({"error": "Internal Server Error."}), 500

    processed_rows = []
    try:
        for i, record in enumerate(symbols_data):
            if not isinstance(record, list) or len(record) != 4:
                error_message = f"Record at index {i} has invalid structure. Expected a list of 4 elements. Found: {record}"
                logger.error(error_message)
                return jsonify(
                    {"error": "Invalid record structure.", "details": error_message}
                ), 400

            ticker, value, market, date_string = record

            # Validate ticker (symbol[0])
            if not isinstance(ticker, str) or not ticker.strip():
                error_message = f"Invalid ticker at index {i}: must be a non-empty string. Found: {ticker}"
                logger.error(error_message)
                return jsonify(
                    {"error": "Invalid data.", "details": error_message}
                ), 400

            # Validate value (symbol[1])
            if not isinstance(value, (int, float)):
                try:
                    value = float(
                        value
                    )  # Try to convert if it's a string representation of a number
                except (ValueError, TypeError):
                    error_message = (
                        f"Invalid value at index {i}: must be a number. Found: {value}"
                    )
                    logger.error(error_message)
                    return jsonify(
                        {"error": "Invalid data.", "details": error_message}
                    ), 400

            # Validate market (symbol[2])
            if not isinstance(market, str) or not market.strip():
                error_message = f"Invalid market at index {i}: must be a non-empty string. Found: {market}"
                logger.error(error_message)
                return jsonify(
                    {"error": "Invalid data.", "details": error_message}
                ), 400

            # Validate date_string (symbol[3])
            try:
                datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                error_message = f"Invalid date format for record at index {i}: '{date_string}'. Expected 'YYYY-MM-DDTHH:MM:SS'."
                logger.error(error_message)
                return jsonify(
                    {"error": "Invalid date format.", "details": error_message}
                ), 400

            processed_rows.append([ticker, value, market, date_string])

    except Exception:  # Catch any unexpected errors during processing
        logger.exception("An unexpected error occurred during symbol processing")
        return jsonify({"error": "Internal Server Error."}), 500

    if not processed_rows:
        info_message = "No valid rows to load after processing."
        logger.info(info_message)
        return jsonify({"message": "No valid data to load."}), 200

    # Prepare CSV data in memory
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    for row in processed_rows:
        csv_writer.writerow(row)
    csv_buffer.seek(0)  # Rewind buffer to the beginning

    try:
        client = bigquery.Client()
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=0,
            autodetect=False,
        )

        job = client.load_table_from_file(csv_buffer, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)
        info_message = f"Loaded {len(processed_rows)} rows to {table_id}. Total rows in table: {table.num_rows}."
        logger.info(info_message)
    except Exception:
        logger.exception("Error loading data into BigQuery:")
        return jsonify({"error": "Internal Server Error."}), 500

    success_message = f"{len(processed_rows)} records loaded successfully."
    return jsonify({"message": success_message}), 200


def bq_batch_load_handler(request):
    if request.method != "POST" or request.content_type != "application/json":
        logger.error("Only POST/json requests are accepted")

        return jsonify({"error": "Only POST/json requests are accepted."}), 405

    data = request.get_json()
    if not data or not isinstance(data.get("symbols"), list):
        error_message = "Invalid request payload: 'symbols' key missing or not a list."
        logger.error(error_message)
        return jsonify(
            {"error": "Invalid request payload: 'symbols' must be a list."}
        ), 400

    return bq_load_data(data["symbols"])
