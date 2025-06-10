import io
import logging
import os
from datetime import datetime

import functions_framework
from dotenv import load_dotenv
from flask import jsonify
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@functions_framework.http
def bq_load_data(request):
    if request.method != "POST" or request.content_type != "application/json":
        logger.error("Only POST/json requests are accepted")

        return jsonify({"error": "Only POST/json requests are accepted."}), 405

    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
    BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

    if not all([BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
        logger.error(
            "Missing environment variables (BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)."
        )

        return jsonify({"error": "Internal Server Error."}), 500

    data = request.get_json()
    if not data or "symbols" not in data:
        logger.error("Invalid request payload, missing symbols data.")

        return jsonify({"error": "Invalid request payload."}), 400

    symbols = data["symbols"]
    try:
        for symbol in symbols:
            if len(symbol) != 4:
                error_message = "Invalid symbol structure: %s. Expected 4 elements." % (
                    symbol,
                )
                logger.error(error_message)

                return jsonify(
                    {"error": "Missing parameters.", "details": error_message}
                ), 400

            try:
                date_string = symbol[3]
                datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                safe_error_detail = (
                    "The provided date '%s' does not match the required format 'YYYY-MM-DDTHH:MM:SS'."
                    % (date_string,)
                )
                logger.exception(safe_error_detail)

                return jsonify(
                    {"error": "Invalid date format.", "details": safe_error_detail}
                ), 400

            file_contents = "%s,%s,%s,%s" % (symbol[0], symbol[1], symbol[2], symbol[3])
            fake_file = io.StringIO(file_contents)

    except Exception:
        logger.exception("An unexpected error occurred during symbol processing:")

        return jsonify({"error": "Internal Server Error."}), 500

    try:
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=0,
            autodetect=False,
        )

        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        job = client.load_table_from_file(fake_file, table_id, job_config=job_config)

        job.result()

        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    except Exception:
        logger.exception("Error loading data into BigQuery:")

        return jsonify({"error": "Internal Server Error."}), 500

    return "Data loaded successfully.", 200
