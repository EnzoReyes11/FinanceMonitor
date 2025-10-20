import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

from alphavantage.client import AlphaVantageClient
from dotenv import load_dotenv
from google.cloud import bigquery, storage
from google.cloud.exceptions import Forbidden, NotFound

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

ALPHA_VANTAGE_API_TOKEN = os.environ.get("ALPHA_VANTAGE_API_TOKEN")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "enzoreyes-financemonitor-dev-financemonitor-data")
BQ_PROJECT = os.environ.get("BQ_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET_ID", "monitor")
MODE = os.environ.get("MODE", "daily")  # "daily" or "backfill"
RUN_DATE = os.environ.get("RUN_DATE")
DIRECTORY = 'alphavantage'

class AlphaVantageExtractor:
    """
    Extracts stock data from Alpha Vantage API and stores raw data in GCS.
    
    This service is designed to be a single node in an Airflow DAG.
    It does NOT load to BigQuery - that's handled by alphavantage_loader.
    """
    
    def __init__(self):
        if not ALPHA_VANTAGE_API_TOKEN:
            raise ValueError("ALPHA_VANTAGE_API_TOKEN not set")
        
        self.client = AlphaVantageClient(ALPHA_VANTAGE_API_TOKEN, logger=logger)
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client() 
        self.bucket = self.storage_client.bucket(GCS_BUCKET)
        self.run_date = RUN_DATE or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
    def get_symbols_to_process(self) -> List[tuple[str, str, str]]:
        """Read symbols from BigQuery that need processing"""
        query = f"""
            SELECT DISTINCT ticker_symbol, exchange_country, exchange_code
            FROM `{BQ_PROJECT}.{BQ_DATASET}.dim_asset`
            WHERE 
              exchange_country = 'US' 
              AND is_active = TRUE 
              AND asset_type = 'STOCK'
            ORDER BY ticker_symbol
        """
        
        try:
            results = self.bq_client.query(query).result()
            symbols = [(row.ticker_symbol, row.exchange_country, row.exchange_code) for row in results]
            logger.info(f"Found {len(symbols)} symbols to process")
            return symbols
        except Exception as e:
            logger.warning(f"Could not read from BQ, using defaults: {e}")
            return [("GOOG", "US", "NASDAQ")] 
    
    def extract_symbol(self, symbol: str, country: str, exchange: str) -> Optional[str]:
        """
        Extract data for a single symbol and write to GCS.
        
        Returns:
            GCS URI if successful, None otherwise
        """
        blob_path = ''
        gcs_uri = ''

        try:
            logger.info(f"Extracting {MODE} data for {symbol}")
            
            data = self.client.get_short_backfill(symbol)

            if data.empty:
                logger.warning(f"No data returned for {symbol}")
                return None

            data['ticker_symbol'] = symbol
            data['exchange_name'] = exchange
            data['country'] = country
            
            if MODE == 'daily':
                data = data.head(1)

            blob_path = f"raw/{MODE}/{DIRECTORY}/{country}_{exchange}_{symbol}/{self.run_date}.csv"
        
            logger.debug('BUCKET ' + GCS_BUCKET)
            # Upload to GCS
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(data.to_csv(index=False), content_type="text/csv")

            # Add metadata for tracking
            blob.metadata = {
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "mode": MODE,
                "symbol": symbol
            }
            blob.patch()

            gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
            logger.info(f"✅ Uploaded {symbol} to {gcs_uri}")
            return gcs_uri

            
        except NotFound as e:
            logger.error(f"Bucket '{gcs_uri}' or blob '{blob_path}' not found. {e}", exc_info=True)
            return None
        except Forbidden as e:
            logger.error(f"Permission forbidden on  '{gcs_uri}' or blob '{blob_path}'. {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ Failed to extract {symbol}: {e}", exc_info=True)
            return None
    
    def run(self):
        """Execute the extraction pipeline"""
        logger.info(f"🚀 Starting extraction in {MODE} mode for {self.run_date}")
        
        symbols = self.get_symbols_to_process()
        print(symbols)

        results = {
            "success": [],
            "failed": [],
            "total": len(symbols)
        }
        
        for (symbol, country, exchange) in symbols:
            gcs_uri = self.extract_symbol(symbol, country, exchange)
            
            if gcs_uri:
                results["success"].append({
                    "symbol": symbol,
                    "country": country,
                    "exchange": exchange,
                    "gcs_uri": gcs_uri
                })
            else:
                results["failed"].append({
                    "symbol": symbol,
                    "country": country,
                    "exchange": exchange,
                    })
        
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
        manifest_path = f"manifests/{MODE}/{DIRECTORY}/{self.run_date}.json"
        blob = self.bucket.blob(manifest_path)
        
        manifest = {
            "run_date": self.run_date,
            "mode": MODE,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "results": results
        }
        
        blob.upload_from_string(
            json.dumps(manifest, indent=2),
            content_type="application/json"
        )
        
        logger.info(f"Wrote manifest to gs://{GCS_BUCKET}/{manifest_path}")


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

