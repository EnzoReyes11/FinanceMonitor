import json
import logging
import os
import sys
from datetime import datetime
from typing import List

from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
GCS_BUCKET = os.environ.get("GCS_BUCKET", "financemonitor-data")
BQ_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "stocks")
BQ_TABLE = os.environ.get("BQ_TABLE", "daily_prices")
MODE = os.environ.get("MODE", "daily")  # "daily" or "backfill"
RUN_DATE = os.environ.get("RUN_DATE")  # For Airflow templating


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
        self.run_date = RUN_DATE or datetime.utcnow().strftime("%Y-%m-%d")
        self.table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    def get_files_to_load(self) -> List[str]:
        """
        Get list of CSV files to load from GCS.
        
        Can work in two modes:
        1. Read from manifest file (preferred)
        2. List files directly from GCS prefix
        """
        # Try to read manifest first
        manifest_path = f"manifests/{MODE}/{self.run_date}.json"
        
        try:
            blob = self.bucket.blob(manifest_path)
            manifest_data = json.loads(blob.download_as_text())
            
            gcs_uris = [
                item["gcs_uri"] 
                for item in manifest_data["results"]["success"]
            ]
            
            logger.info(f"📝 Loaded {len(gcs_uris)} files from manifest")
            return gcs_uris
            
        except Exception as e:
            logger.warning(f"Could not read manifest, listing files directly: {e}")
            return self._list_files_from_gcs()
    
    def _list_files_from_gcs(self) -> List[str]:
        """Fallback: list all CSV files for the run date"""
        prefix = f"raw/{MODE}/"
        
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
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("open", "FLOAT64"),
                    bigquery.SchemaField("high", "FLOAT64"),
                    bigquery.SchemaField("low", "FLOAT64"),
                    bigquery.SchemaField("close", "FLOAT64"),
                    bigquery.SchemaField("volume", "INTEGER"),
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
        
        for gcs_uri in gcs_files:
            success = self.load_file_to_bq(gcs_uri)
            
            if success:
                results["success"].append(gcs_uri)
                # Optionally move to processed
                # self.move_to_processed(gcs_uri)
            else:
                results["failed"].append(gcs_uri)
        
        # Log summary
        total = len(gcs_files)
        success_count = len(results["success"])
        logger.info(f"📊 Load complete: {success_count}/{total} files loaded")
        
        if results["failed"]:
            logger.warning(f"⚠️  Failed files: {results['failed']}")
        
        # Return 0 if at least one file loaded successfully
        return 0 if results["success"] else 1


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