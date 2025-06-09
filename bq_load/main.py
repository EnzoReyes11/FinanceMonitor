import io
import os

import functions_framework
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


@functions_framework.http
def bq_load_data(request):
    if request.method != "POST":
        return "Only POST requests are accepted", 405

    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
    BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

    if not all([BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID]):
        return "Server configuration error: Missing BigQuery identifiers", 500

    # Construct a BigQuery client object.
    client = bigquery.Client()

    file_contents = "SPY,500,US,2025-06-09T16:42:31.190280"
    fake_file = io.StringIO(file_contents)

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
