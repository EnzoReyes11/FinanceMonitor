import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

from dotenv import load_dotenv
from google.cloud import bigquery, storage
from google.cloud import exceptions as google_exceptions

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
GCS_BUCKET = os.environ.get("GCS_BUCKET", "enzoreyes-financemonitor-dev-financemonitor-data")
BQ_PROJECT = os.environ.get("BQ_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET_ID", "monitor")
MODE = os.environ.get("MODE", "daily")  # "daily" or "backfill"
RUN_DATE = os.environ.get("RUN_DATE")  
DIRECTORY = 'alphavantage'


class AlphaVantageLoader:
    """
    Loads stock data from GCS to BigQuery.
    
    This service is designed to be a downstream node in an Airflow DAG,
    running after alphavantage_extractor has written data to GCS.
    """
    
    def __init__(self):
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.bucket = self.storage_client.bucket(GCS_BUCKET)
        utc_minus_3 = timezone(timedelta(hours=-3))
        self.run_date = RUN_DATE or datetime.now(utc_minus_3).strftime("%Y-%m-%d")
        self.table_id = f"{BQ_PROJECT}.{BQ_DATASET}.fact_price_history"
    

    def _load_and_transform(self, project_id, dataset_id, gcs_uris, schema, transform_query):
        """Helper function to load data to a temp table and run a transform query."""
        if not gcs_uris:
            return

        client = self.bq_client

        temp_table_name = f"temp_source_{uuid.uuid4().hex}"
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("timestamp", "DATE"),
                    bigquery.SchemaField("open", "FLOAT64"),
                    bigquery.SchemaField("high", "FLOAT64"),
                    bigquery.SchemaField("low", "FLOAT64"),
                    bigquery.SchemaField("close", "FLOAT64"),
                    bigquery.SchemaField("volume", "INTEGER"),
                    bigquery.SchemaField("ticker_symbol", "STRING"),
                    bigquery.SchemaField("exchange_name", "STRING"),
                    bigquery.SchemaField("country", "STRING"),
                    bigquery.SchemaField("is_adjusted", "BOOLEAN"),
                ],
            )

            logger.info(f"Loading {len(gcs_uris)} rows into temporary table {temp_table_id}")
            for gcs_uri in gcs_uris:
                try:
                    load_job = self.bq_client.load_table_from_uri(
                        gcs_uri, 
                        temp_table_id,
                        job_config=job_config
                    )

                    load_job.result()
            
                except Exception:
                    logger.exception('Error loading CSV into temp table', gcs_uri)
                    return


            fact_price_history_table_id = f"{project_id}.{dataset_id}.fact_price_history"
    
            insert_query = f"""
                INSERT INTO `{fact_price_history_table_id}` (
                    asset_key, snapshot_date, snapshot_timestamp, ingestion_timestamp,
                    open_price, high_price, low_price, close_price, 
                    volume, is_adjusted, data_source
                    )
                SELECT
                    FARM_FINGERPRINT(S.ticker_symbol || S.exchange_name) AS asset_key,
                    CAST(S.timestamp AS DATE) AS snapshot_date,
                    CAST(S.timestamp AS TIMESTAMP) AS snapshot_timestamp,
                    CURRENT_TIMESTAMP() AS ingestion_timestamp,
                    CAST(S.open AS NUMERIC) AS open_price,
                    CAST(S.close AS NUMERIC) AS close_price,
                    CAST(S.high AS NUMERIC) AS high_price,
                    CAST(S.low AS NUMERIC) AS low_price,
                    CAST(S.volume AS NUMERIC) AS volume,
                    CAST(S.is_adjusted AS BOOLEAN) AS is_adjusted,
                    'alphavantage' AS data_source
                FROM `{{temp_table_id}}` S
            """
            # Execute the main transform query (MERGE or INSERT)
            final_query = insert_query.format(temp_table_id=temp_table_id)
            logger.info(f"Executing transform query into {fact_price_history_table_id}")
            logger.debug(final_query)
            query_job = client.query(final_query)
            query_job.result()
            logger.info("Transform query completed successfully.")

        except Exception as e:
            logger.error(f"Error during BigQuery load/transform process: {e}")
        #    logger.info(final_query)
            raise 
        #finally:
            #logger.info(f"Deleting temporary table {temp_table_id}")
            #client.delete_table(temp_table_id, not_found_ok=True)



    def get_files_to_load(self) -> List[str]:
        """
        Get list of CSV files to load from GCS.
        
        Can work in two modes:
        1. Read from manifest file (preferred)
        2. List files directly from GCS prefix
        """
        # Try to read manifest first
        #/manifests/daily/alphavantage/2025-10-20.json
        manifest_path = f"manifests/{MODE}/{DIRECTORY}/{self.run_date}.json"
        
        try:
            blob = self.bucket.blob(manifest_path)
            manifest_data = json.loads(blob.download_as_text())
            
            gcs_uris = [
                item["gcs_uri"] 
                for item in manifest_data["results"]["success"]
            ]
            
            logger.info(f"📝 Loaded {len(gcs_uris)} files from manifest")
            return gcs_uris
            
        except google_exceptions.NotFound:
            logger.error(f"Manifest file not found: {manifest_path}")
            return []
        except Exception:
            logger.exception(f"Error loading manifest file: {manifest_path}")
            return []

            #return self._list_files_from_gcs()
    
    def _list_files_from_gcs(self) -> List[str]:
        """Fallback: list all CSV files for the run date"""
        prefix = f"raw/{MODE}/{DIRECTORY}"
        
        blobs = self.bucket.list_blobs(prefix=prefix)
        gcs_uris = []
        
        for blob in blobs:
            # Filter by run date and CSV extension
            if self.run_date in blob.name and blob.name.endswith('.csv'):
                gcs_uris.append(f"gs://{GCS_BUCKET}/{blob.name}")
        
        logger.info(f"📂 Found {len(gcs_uris)} files in GCS")
        return gcs_uris
    
    def load_file_to_bq(self, gcs_uri: str) -> bool:
        """
        Load a single CSV file from GCS to BigQuery.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading {gcs_uri} to {self.table_id}")
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,  # Use explicit schema
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("asset_key", "INTEGER"),
                    bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("snapshotDate", "DATE"),
                    bigquery.SchemaField("open_price", "FLOAT64"),
                    bigquery.SchemaField("high_price", "FLOAT64"),
                    bigquery.SchemaField("low_price", "FLOAT64"),
                    bigquery.SchemaField("close_price", "FLOAT64"),
                    bigquery.SchemaField("volume", "INTEGER"),
                    bigquery.SchemaField("datasource", "STRING"),
                    bigquery.SchemaField("is_adjusted", "BOOLEAN"),
                ],
                # Optional: add symbol from filename
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            )
            
            # Start load job
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri, 
                self.table_id, 
                job_config=job_config
            )
            
            # Wait for completion
            load_job.result()
            
            # Log stats
            dest_table = self.bq_client.get_table(self.table_id)
            logger.info(f"✅ Loaded {load_job.output_rows} rows from {gcs_uri}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load {gcs_uri}: {e}", exc_info=True)
            return False
    
    def move_to_processed(self, gcs_uri: str):
        """Move successfully loaded files to processed folder"""
        try:
            # Extract blob path from URI
            blob_path = gcs_uri.replace(f"gs://{GCS_BUCKET}/", "")
            source_blob = self.bucket.blob(blob_path)
            
            # Create destination path
            dest_path = blob_path.replace("raw/", "processed/")
            dest_blob = self.bucket.blob(dest_path)
            
            # Copy and delete
            self.bucket.copy_blob(source_blob, self.bucket, dest_path)
            source_blob.delete()
            
            logger.info(f"📦 Moved {blob_path} to {dest_path}")
            
        except Exception as e:
            logger.warning(f"Could not move file to processed: {e}")
    
    def run(self):
        """Execute the loading pipeline"""
        logger.info(f"🚀 Starting load for {MODE} mode, date: {self.run_date}")
        
        gcs_files = self.get_files_to_load()
        
        if not gcs_files:
            logger.warning("⚠️  No files to load")
            return 1
        
        results = {
            "success": [],
            "failed": []
        }

        self._load_and_transform(BQ_PROJECT, BQ_DATASET, gcs_files, '', '')
        
   #     for gcs_uri in gcs_files:
   #         success = self.load_file_to_bq(gcs_uri)
   #         
   #         if success:
   #             results["success"].append(gcs_uri)
   #             # Optionally move to processed
   #             # self.move_to_processed(gcs_uri)
   #         else:
   #             results["failed"].append(gcs_uri)
   #     
   #     # Log summary
   #     total = len(gcs_files)
   #     success_count = len(results["success"])
   #     logger.info(f"📊 Load complete: {success_count}/{total} files loaded")
   #     
   #     if results["failed"]:
   #         logger.warning(f"⚠️  Failed files: {results['failed']}")
   #     
   #     # Return 0 if at least one file loaded successfully
   #     return 0 if results["success"] else 1


def main():
    """Entry point for Cloud Run"""
    try:
        loader = AlphaVantageLoader()
        exit_code = loader.run()
        
        if exit_code == 0:
            logger.info("✅ Load completed successfully")
        else:
            logger.error("❌ Load failed")
        
        return exit_code
        
    except Exception as e:
        logger.exception("💥 Fatal error in load pipeline")
        print(json.dumps({
            "message": f"Load failed: {str(e)}",
            "severity": "ERROR"
        }))
        return 1


if __name__ == "__main__":
    sys.exit(main())