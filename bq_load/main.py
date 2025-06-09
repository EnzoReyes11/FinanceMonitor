import io
import logging
import os

import functions_framework
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@functions_framework.http
def bq_load_data(request):
    if request.method != "POST":
        logger.error("Only POST requests are accepted")

        return "Only POST requests are accepted", 405

    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
    BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

    if not all([BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
        logger.error(
            "Missing environment variables (BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)."
        )

        return "Server configuration error.", 500

    try:
        file_contents = "SPY,700,US,2025-06-09T16:42:31.190280"
        fake_file = io.StringIO(file_contents)
    except Exception:
        logger.exception("Error creating fake file:")

        return "Internal server error.", 500

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

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    except Exception:
        logger.exception("Error loading data into BigQuery:")
        return "Internal server error.", 500

    return "Data loaded successfully.", 200
