import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery


def _load_and_transform(client, project_id, dataset_id, rows, schema, transform_query):
    """Helper function to load data to a temp table and run a transform query."""
    if not rows:
        return

    temp_table_name = f"temp_source_{uuid.uuid4().hex}"
    temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    final_query = ''
    try:
        # Load data into the temporary table
        logging.info(f"Loading {len(rows)} rows into temporary table {temp_table_id}")
        load_job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
        load_job.result()

        # Set an expiration on the temp table for auto-cleanup
        temp_table = client.get_table(temp_table_id)
        temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
        client.update_table(temp_table, ["expires"])

        # Execute the main transform query (MERGE or INSERT)
        final_query = transform_query.format(temp_table_id=temp_table_id)
        logging.info("Executing transform query...")
        query_job = client.query(final_query)
        query_job.result()
        logging.info("Transform query completed successfully.")

    except Exception as e:
        logging.error(f"Error during BigQuery load/transform process: {e}")
        logging.info(final_query)
        raise 
    finally:
        logging.info(f"Deleting temporary table {temp_table_id}")
        client.delete_table(temp_table_id, not_found_ok=True)



def load_data_to_bigquery(fixed_income_rows, daily_values_rows, dry_run=False):
    """
    Loads transformed data into BigQuery tables using a temporary table for the MERGE operation.
    """
    if not fixed_income_rows and not daily_values_rows:
        logging.info("No new data to load to BigQuery.")
        return

    project_id = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise RuntimeError("Missing BQ project id. Set BQ_PROJECT_ID or GOOGLE_CLOUD_PROJECT.")

    dataset_id = "financeTools"
    fixed_income_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income"
    daily_values_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income_daily_values"

    if dry_run:
        logging.info("--- BigQuery Dry Run ---")
        if fixed_income_rows:
            logging.info(f"Would MERGE {len(fixed_income_rows)} rows into {fixed_income_table_id}")
        if daily_values_rows:
            logging.info(f"Would INSERT {len(daily_values_rows)} rows into {daily_values_table_id}")
        return

    client = bigquery.Client()

    if fixed_income_rows:
        fixed_income_schema = [
            bigquery.SchemaField("ticker_symbol", "STRING"),
            bigquery.SchemaField("issue_date", "STRING"),
            bigquery.SchemaField("payment_date", "STRING"),
            bigquery.SchemaField("amount_at_payment", "STRING"),
            bigquery.SchemaField("rate", "STRING"),
            bigquery.SchemaField("type", "STRING"),
        ]

        merge_query = f"""
                MERGE `{fixed_income_table_id}` T
                USING `{{temp_table_id}}` S
                ON T.asset_key = FARM_FINGERPRINT(S.ticker_symbol || '|byma')
                WHEN MATCHED THEN
                  UPDATE SET
                    T.issue_date = CAST(S.issue_date AS DATE),
                    T.payment_date = CAST(S.payment_date AS DATE),
                    T.amount_at_payment = CAST(S.amount_at_payment AS NUMERIC),
                    T.rate = CAST(S.rate AS NUMERIC),
                    T.type = S.type
                WHEN NOT MATCHED THEN
                  INSERT (asset_key, ticker_symbol, issue_date, payment_date, amount_at_payment, rate, type)
                  VALUES(
                    FARM_FINGERPRINT(S.ticker_symbol || '|byma'),
                    S.ticker_symbol,
                    CAST(S.issue_date AS DATE),
                    CAST(S.payment_date AS DATE),
                    CAST(S.amount_at_payment AS NUMERIC),
                    CAST(S.rate AS NUMERIC),
                    S.type
                  );
            """

        _load_and_transform(client, project_id, dataset_id, fixed_income_rows, fixed_income_schema, merge_query)


    if daily_values_rows:
        daily_values_schema = [
            bigquery.SchemaField("ticker_symbol", "STRING"),
            bigquery.SchemaField("snapshot_date", "STRING"),
            bigquery.SchemaField("ingestion_timestamp", "STRING"),
            bigquery.SchemaField("maturity_value", "STRING"),
            bigquery.SchemaField("action_rate", "STRING"),
            bigquery.SchemaField("price_per_100_nominal_value", "STRING"),
            bigquery.SchemaField("period_yield", "STRING"),
            bigquery.SchemaField("annual_percentage_rate", "STRING"),
            bigquery.SchemaField("effective_annual_rate", "STRING"),
            bigquery.SchemaField("effective_monthly_rate", "STRING"),
            bigquery.SchemaField("modified_duration_in_days", "INTEGER"),
        ]

        insert_query = f"""
            INSERT INTO `{daily_values_table_id}` (
                asset_key, ticker_symbol, snapshot_date, ingestion_timestamp,
                maturity_value, action_rate, price_per_100_nominal_value, period_yield,
                annual_percentage_rate, effective_annual_rate, effective_monthly_rate,
                modified_duration_in_days)
            SELECT
                FARM_FINGERPRINT(S.ticker_symbol || '|byma') AS asset_key,
                S.ticker_symbol,
                CAST(S.snapshot_date AS DATE) AS snapshot_date,
                CAST(S.ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
                CAST(S.maturity_value AS NUMERIC) AS maturity_value,
                CAST(S.action_rate AS NUMERIC) AS action_rate,
                CAST(S.price_per_100_nominal_value AS NUMERIC) AS price_per_100_nominal_value,
                CAST(S.period_yield AS NUMERIC) AS period_yield,
                CAST(S.annual_percentage_rate AS NUMERIC) AS annual_percentage_rate,
                CAST(S.effective_annual_rate AS NUMERIC) AS effective_annual_rate,
                CAST(S.effective_monthly_rate AS NUMERIC) AS effective_monthly_rate,
                S.modified_duration_in_days
            FROM `{{temp_table_id}}` S
        """
        _load_and_transform(client, project_id, dataset_id, daily_values_rows, daily_values_schema, insert_query)