import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

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

    def get_files_to_load(self) -> list[str]:
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
    
    
    def move_to_processed(self, gcs_uri: str):
        """Move successfully loaded files to processed folder"""
        try:
            blob_path = gcs_uri.replace(f"gs://{GCS_BUCKET}/", "")
            source_blob = self.bucket.blob(blob_path)
            
            dest_path = blob_path.replace("raw/", "processed/")
            dest_blob = self.bucket.blob(dest_path)
            
            self.bucket.copy_blob(source_blob, self.bucket, dest_path)
            source_blob.delete()
            
            logger.info(f"📦 Moved {blob_path} to {dest_path}")
            
        except Exception as e:
            logger.exception(f"Could not move file to processed:")

   
    def _load_to_temp(self, gcs_uris: list[str]) -> dict[str, Any]:
        """
        Load all CSV files from GCS into a single temporary table.
        
        Returns dict with success/failure info and temp_table_id.
        """
        if not gcs_uris:
            return {"success": False, "temp_table_id": None, "failed_files": []}
        
        temp_table_name = f"temp_av_load_{uuid.uuid4().hex}"
        temp_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{temp_table_name}"
        self.temp_table_id = temp_table_id
        
        results = {
            "success": True,
            "temp_table_id": temp_table_id,
            "loaded_files": [],
            "failed_files": []
        }
        
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
        
        logger.info(f"📥 Loading {len(gcs_uris)} files into temp table {temp_table_id}")
        
        for gcs_uri in gcs_uris:
            try:
                logger.info(f"  Loading {gcs_uri}")
                
                load_job = self.bq_client.load_table_from_uri(
                    gcs_uri,
                    temp_table_id,
                    job_config=job_config
                )
                
                load_job.result()
                
                rows_loaded = load_job.output_rows or 0
                logger.info(f"  ✅ Loaded {rows_loaded} rows")
                results["loaded_files"].append(gcs_uri)
                
            except google_exceptions.NotFound:
                logger.exception(f"  ❌ File not found: {gcs_uri}")
                results["failed_files"].append(gcs_uri)
            except Exception as e:
                logger.exception(f"  ❌ Failed to load {gcs_uri}")
                results["failed_files"].append(gcs_uri)
        
        if not results["loaded_files"]:
            results["success"] = False
            logger.error("❌ No files loaded successfully")
        else:
            logger.info(f"✅ Loaded {len(results['loaded_files'])}/{len(gcs_uris)} files to temp table")
        
        return results
    
    def _transform_and_insert(self, temp_table_id: str) -> bool:
        """
        Transform data from temp table and insert into fact_price_history.
        Uses your original query logic.
        """
        fact_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.fact_price_history"
        
        insert_query = f"""
            INSERT INTO `{fact_table_id}` (
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
                CAST(S.high AS NUMERIC) AS high_price,
                CAST(S.low AS NUMERIC) AS low_price,
                CAST(S.close AS NUMERIC) AS close_price,
                CAST(S.volume AS INT64) AS volume,
                CAST(S.is_adjusted AS BOOLEAN) AS is_adjusted,
                'alphavantage' AS data_source
            FROM `{temp_table_id}` S
        """
        
        try:
            logger.info(f"🔄 Transforming and inserting data into {fact_table_id}")
            logger.debug(f"Query: {insert_query}")
            
            query_job = self.bq_client.query(insert_query)
            query_job.result()
            
            rows_inserted = query_job.num_dml_affected_rows or 0
            logger.info(f"✅ Successfully inserted {rows_inserted} rows")
            
            return True
            
        except Exception as e:
            logger.exception(f"❌ Transform/insert failed:")
            return False
    
    def _move_to_processed(self, gcs_uris: list[str]) -> dict[str, list[str]]:
        """
        Move successfully loaded files from raw/ to processed/ folder.
        
        Returns dict with 'moved' and 'failed' lists.
        """
        results = {"moved": [], "failed": []}
        
        logger.info(f"📦 Moving {len(gcs_uris)} files to processed folder")
        
        for gcs_uri in gcs_uris:
            try:
                # Extract blob path from URI
                blob_path = gcs_uri.replace(f"gs://{GCS_BUCKET}/", "")
                source_blob = self.bucket.blob(blob_path)
                
                # Create destination path (raw/ -> processed/)
                dest_path = blob_path.replace("raw/", "processed/")
                
                dest_blob = self.bucket.copy_blob(
                    source_blob,
                    self.bucket,
                    dest_path
                )
                
                source_blob.delete()
                
                logger.info(f"  ✅ Moved {blob_path} → {dest_path}")
                results["moved"].append(gcs_uri)
                
            except google_exceptions.NotFound:
                logger.warning(f"  ⚠️  Source file not found: {gcs_uri}")
                results["failed"].append(gcs_uri)
            except Exception as e:
                logger.error(f"  ❌ Failed to move {gcs_uri}: {e}", exc_info=True)
                results["failed"].append(gcs_uri)
        
        logger.info(f"📦 Moved {len(results['moved'])}/{len(gcs_uris)} files")
        
        if results["failed"]:
            logger.warning(f"⚠️  Failed to move: {results['failed']}")
        
        return results
    
    def _cleanup_temp_table(self):
        """Delete the temporary table"""
        if not self.temp_table_id:
            return
        
        try:
            logger.info(f"🧹 Deleting temporary table {self.temp_table_id}")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete temp table: {e}")
    
    def run(self) -> int:
        """
        Execute the loading pipeline.
        
        Returns 0 on success, 1 on failure.
        """
        logger.info(f"🚀 Starting load for {MODE} mode, date: {self.run_date}")
        
        try:
            # Step 1: Get files to load
            gcs_files = self.get_files_to_load()
            
            if not gcs_files:
                logger.warning("⚠️  No files to load")
                return 1
            
            # Step 2: Load all files to single temp table
            load_results = self._load_to_temp(gcs_files)
            
            if not load_results["success"]:
                logger.error("❌ Failed to load files to temp table")
                return 1
            
            # Step 3: Transform and insert into final table
            transform_success = self._transform_and_insert(load_results["temp_table_id"])
            
            if not transform_success:
                logger.error("❌ Transform/insert failed")
                return 1

            # Step 4: Move successfully loaded files to processed folder
            move_results = self._move_to_processed(load_results["loaded_files"])
            
            # Log summary
            total = len(gcs_files)
            success_count = len(load_results["loaded_files"])
            moved_count = len(move_results["moved"])
            
            logger.info(f"📊 Pipeline complete:")
            logger.info(f"   Files loaded: {success_count}/{total}")
            logger.info(f"   Files moved: {moved_count}/{success_count}")
            
            if load_results["failed_files"]:
                logger.warning(f"⚠️  Failed files: {load_results['failed_files']}")

            if move_results["failed"]:
                logger.warning(f"⚠️  Failed to move: {move_results['failed']}")
            
            return 0
            
        except Exception as e:
            logger.exception(f"💥 Fatal error in pipeline: {e}")
            return 1
        
        finally:
            # Always cleanup temp table
            self._cleanup_temp_table()


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
        logger.info(json.dumps({
            "message": f"Load failed: {str(e)}",
            "severity": "ERROR"
        }))
        return 1


if __name__ == "__main__":
    sys.exit(main())
