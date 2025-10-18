import json
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional

from alphavantage.client import AlphaVantageClient
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
ALPHA_VANTAGE_API_TOKEN = os.environ.get("ALPHA_VANTAGE_API_TOKEN")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "financemonitor-data")
BQ_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "stocks")
MODE = os.environ.get("MODE", "daily")  # "daily" or "backfill"
RUN_DATE = os.environ.get("RUN_DATE")  # Optional: for Airflow templating


class AlphaVantageExtractor:
    """
    Extracts stock data from Alpha Vantage API and stores raw data in GCS.
    
    This service is designed to be a single node in an Airflow DAG.
    It does NOT load to BigQuery - that's handled by alphavantage_loader.
    """
    
    def __init__(self):
        if not ALPHA_VANTAGE_API_TOKEN:
            raise ValueError("ALPHA_VANTAGE_API_TOKEN not set")
        
        self.client = AlphaVantageClient(ALPHA_VANTAGE_API_TOKEN)
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()  # Only for reading symbols
        self.bucket = self.storage_client.bucket(GCS_BUCKET)
        self.run_date = RUN_DATE or datetime.utcnow().strftime("%Y-%m-%d")
        
    def get_symbols_to_process(self) -> List[str]:
        """Read symbols from BigQuery that need processing"""
        query = f"""
            SELECT DISTINCT symbol
            FROM `{BQ_PROJECT}.{BQ_DATASET}.symbols_master`
            WHERE active = TRUE
            ORDER BY symbol
        """
        
        try:
            results = self.bq_client.query(query).result()
            symbols = [row.symbol for row in results]
            logger.info(f"Found {len(symbols)} symbols to process")
            return symbols
        except Exception as e:
            logger.warning(f"Could not read from BQ, using defaults: {e}")
            # Fallback for development/testing
            return ["GOOGL", "AAPL", "MSFT"]
    
    def extract_symbol(self, symbol: str) -> Optional[str]:
        """
        Extract data for a single symbol and write to GCS.
        
        Returns:
            GCS URI if successful, None otherwise
        """
        try:
            logger.info(f"Extracting {MODE} data for {symbol}")
            
            # Get data from API
            data = self.client.get_short_backfill(symbol)
            
            if not data:
                logger.warning(f"No data returned for {symbol}")
                return None
            
            # Determine GCS path based on mode
            if MODE == "backfill":
                blob_path = f"raw/backfill/{symbol}/{self.run_date}.csv"
            else:
                blob_path = f"raw/daily/{symbol}/{self.run_date}.csv"
            
            # Upload to GCS
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(data, content_type="text/csv")
            
            # Add metadata for tracking
            blob.metadata = {
                "extracted_at": datetime.utcnow().isoformat(),
                "mode": MODE,
                "symbol": symbol
            }
            blob.patch()
            
            gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
            logger.info(f"✅ Uploaded {symbol} to {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"❌ Failed to extract {symbol}: {e}", exc_info=True)
            return None
    
    def run(self):
        """Execute the extraction pipeline"""
        logger.info(f"🚀 Starting extraction in {MODE} mode for {self.run_date}")
        
        symbols = self.get_symbols_to_process()
        
        results = {
            "success": [],
            "failed": [],
            "total": len(symbols)
        }
        
        for symbol in symbols:
            gcs_uri = self.extract_symbol(symbol)
            
            if gcs_uri:
                results["success"].append({
                    "symbol": symbol,
                    "gcs_uri": gcs_uri
                })
            else:
                results["failed"].append(symbol)
        
        # Log summary
        logger.info(f"📊 Extraction complete: {len(results['success'])}/{results['total']} successful")
        
        if results["failed"]:
            logger.warning(f"⚠️  Failed symbols: {results['failed']}")
        
        # Write manifest file for downstream processing
        self._write_manifest(results)
        
        # Return exit code: 0 if at least one symbol succeeded
        return 0 if results["success"] else 1
    
    def _write_manifest(self, results: dict):
        """Write a manifest file with extraction results for the loader"""
        manifest_path = f"manifests/{MODE}/{self.run_date}.json"
        blob = self.bucket.blob(manifest_path)
        
        manifest = {
            "run_date": self.run_date,
            "mode": MODE,
            "extracted_at": datetime.utcnow().isoformat(),
            "results": results
        }
        
        blob.upload_from_string(
            json.dumps(manifest, indent=2),
            content_type="application/json"
        )
        
        logger.info(f"📝 Wrote manifest to gs://{GCS_BUCKET}/{manifest_path}")


def main():
    """Entry point for Cloud Run"""
    try:
        extractor = AlphaVantageExtractor()
        exit_code = extractor.run()
        
        if exit_code == 0:
            logger.info("✅ Extraction completed successfully")
        else:
            logger.error("❌ Extraction failed")
        
        return exit_code
        
    except Exception as e:
        logger.exception("💥 Fatal error in extraction pipeline")
        print(json.dumps({
            "message": f"Extraction failed: {str(e)}",
            "severity": "ERROR"
        }))
        return 1


if __name__ == "__main__":
    sys.exit(main())

