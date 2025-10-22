"""
Unit tests for AlphaVantageExtractor.

Key concepts:
1. Mock external dependencies (API, GCS, BigQuery)
2. Test business logic in isolation
3. Verify error handling
4. Check edge cases
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import pandas as pd
from google.cloud.exceptions import NotFound, Forbidden

# Import the class we're testing
from alphavantage_extractor.main import AlphaVantageExtractor


# Fixtures - reusable test data and mocks
@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up environment variables for testing"""
    monkeypatch.setenv("ALPHA_VANTAGE_API_TOKEN", "test_token_123")
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("BQ_PROJECT_ID", "test-project")
    monkeypatch.setenv("BQ_DATASET_ID", "test-dataset")
    monkeypatch.setenv("MODE", "daily")
    monkeypatch.setenv("RUN_DATE", "2025-10-22")


@pytest.fixture
def sample_price_data():
    """Sample data returned by Alpha Vantage API"""
    return pd.DataFrame({
        'timestamp': ['2025-10-22', '2025-10-21', '2025-10-20'],
        'open': [150.0, 148.0, 149.0],
        'high': [152.0, 150.0, 151.0],
        'low': [149.0, 147.0, 148.5],
        'close': [151.5, 149.5, 150.5],
        'volume': [1000000, 950000, 1100000]
    })


@pytest.fixture
def sample_symbols():
    """Sample symbols from BigQuery"""
    return [
        ("AAPL", "US", "NASDAQ"),
        ("GOOGL", "US", "NASDAQ"),
        ("MSFT", "US", "NYSE")
    ]


@pytest.fixture
def mock_extractor(mock_env_vars):
    """
    Create an AlphaVantageExtractor with mocked dependencies.
    
    This is the core fixture - it patches all external dependencies.
    """
    with patch('alphavantage_extractor.main.AlphaVantageClient') as mock_av_client, \
         patch('alphavantage_extractor.main.storage.Client') as mock_storage, \
         patch('alphavantage_extractor.main.bigquery.Client') as mock_bq:
        
        # Create the extractor (will use mocked clients)
        extractor = AlphaVantageExtractor()
        
        # Attach mocks for assertions
        extractor.mock_av_client = mock_av_client.return_value
        extractor.mock_storage = mock_storage.return_value
        extractor.mock_bq = mock_bq.return_value
        
        yield extractor


# Test Cases

class TestAlphaVantageExtractorInit:
    """Tests for initialization and configuration"""
    
    def test_init_requires_api_token(self, monkeypatch):
        """Should raise error if API token is missing"""
        monkeypatch.delenv("ALPHA_VANTAGE_API_TOKEN", raising=False)
        
        with pytest.raises(ValueError, match="ALPHA_VANTAGE_API_TOKEN not set"):
            AlphaVantageExtractor()
    
    def test_init_with_valid_token(self, mock_env_vars):
        """Should initialize successfully with valid token"""
        with patch('alphavantage_extractor.main.AlphaVantageClient'), \
             patch('alphavantage_extractor.main.storage.Client'), \
             patch('alphavantage_extractor.main.bigquery.Client'):
            
            extractor = AlphaVantageExtractor()
            assert extractor.run_date == "2025-10-22"
    
    def test_init_uses_current_date_if_no_run_date(self, mock_env_vars, monkeypatch):
        """Should use current date if RUN_DATE not provided"""
        monkeypatch.delenv("RUN_DATE", raising=False)
        
        with patch('alphavantage_extractor.main.AlphaVantageClient'), \
             patch('alphavantage_extractor.main.storage.Client'), \
             patch('alphavantage_extractor.main.bigquery.Client'):
            
            extractor = AlphaVantageExtractor()
            # Should have today's date in some format
            assert extractor.run_date is not None
            assert len(extractor.run_date) == 10  # YYYY-MM-DD format


class TestGetSymbolsToProcess:
    """Tests for getting symbols from BigQuery"""
    
    def test_get_symbols_success(self, mock_extractor, sample_symbols):
        """Should return list of symbols from BigQuery"""
        # Mock BigQuery result
        mock_result = Mock()
        mock_result.result.return_value = [
            Mock(ticker_symbol="AAPL", exchange_country="US", exchange_code="NASDAQ"),
            Mock(ticker_symbol="GOOGL", exchange_country="US", exchange_code="NASDAQ"),
        ]
        mock_extractor.mock_bq.query.return_value = mock_result
        
        # Execute
        symbols = mock_extractor.get_symbols_to_process()
        
        # Assert
        assert len(symbols) == 2
        assert symbols[0] == ("AAPL", "US", "NASDAQ")
        assert symbols[1] == ("GOOGL", "US", "NASDAQ")
        
        # Verify query was called
        mock_extractor.mock_bq.query.assert_called_once()
        query = mock_extractor.mock_bq.query.call_args[0][0]
        assert "dim_asset" in query
        assert "is_active = TRUE" in query
    
    def test_get_symbols_handles_bq_error(self, mock_extractor):
        """Should return empty list if BigQuery fails"""
        # Mock BigQuery error
        mock_extractor.mock_bq.query.side_effect = Exception("BQ connection failed")
        
        # Execute
        symbols = mock_extractor.get_symbols_to_process()
        
        # Assert
        assert symbols == []
    
    def test_get_symbols_empty_result(self, mock_extractor):
        """Should handle empty result from BigQuery"""
        # Mock empty result
        mock_result = Mock()
        mock_result.result.return_value = []
        mock_extractor.mock_bq.query.return_value = mock_result
        
        # Execute
        symbols = mock_extractor.get_symbols_to_process()
        
        # Assert
        assert symbols == []


class TestExtractSymbol:
    """Tests for extracting a single symbol"""
    
    def test_extract_symbol_success(self, mock_extractor, sample_price_data):
        """Should extract data and upload to GCS"""
        # Mock API response
        mock_extractor.client.get_short_backfill.return_value = sample_price_data
        
        # Mock GCS blob
        mock_blob = Mock()
        mock_extractor.bucket.blob.return_value = mock_blob
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Assert
        assert gcs_uri is not None
        assert "gs://test-bucket/raw/daily/alphavantage/US_NASDAQ_AAPL/2025-10-22.csv" in gcs_uri
        
        # Verify API was called
        mock_extractor.client.get_short_backfill.assert_called_once_with("AAPL")
        
        # Verify blob operations
        mock_blob.upload_from_string.assert_called_once()
        upload_data = mock_blob.upload_from_string.call_args[0][0]
        assert "ticker_symbol" in upload_data
        assert "AAPL" in upload_data
        
        # Verify metadata was set
        mock_blob.patch.assert_called_once()
    
    def test_extract_symbol_daily_mode_takes_only_latest(self, mock_extractor, sample_price_data):
        """In daily mode, should only take the first row"""
        mock_extractor.client.get_short_backfill.return_value = sample_price_data
        mock_blob = Mock()
        mock_extractor.bucket.blob.return_value = mock_blob
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Check uploaded data has only 1 row (plus header)
        upload_data = mock_blob.upload_from_string.call_args[0][0]
        lines = upload_data.strip().split('\n')
        assert len(lines) == 2  # header + 1 data row
    
    def test_extract_symbol_empty_data(self, mock_extractor):
        """Should handle empty data from API"""
        # Mock empty DataFrame
        mock_extractor.client.get_short_backfill.return_value = pd.DataFrame()
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("INVALID", "US", "NASDAQ")
        
        # Assert
        assert gcs_uri is None
    
    def test_extract_symbol_api_exception(self, mock_extractor):
        """Should handle API exceptions gracefully"""
        # Mock API error
        mock_extractor.client.get_short_backfill.side_effect = Exception("API rate limit")
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Assert
        assert gcs_uri is None
    
    def test_extract_symbol_gcs_not_found(self, mock_extractor, sample_price_data):
        """Should handle GCS NotFound error"""
        mock_extractor.client.get_short_backfill.return_value = sample_price_data
        mock_blob = Mock()
        mock_blob.upload_from_string.side_effect = NotFound("Bucket not found")
        mock_extractor.bucket.blob.return_value = mock_blob
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Assert
        assert gcs_uri is None
    
    def test_extract_symbol_gcs_forbidden(self, mock_extractor, sample_price_data):
        """Should handle GCS Forbidden error"""
        mock_extractor.client.get_short_backfill.return_value = sample_price_data
        mock_blob = Mock()
        mock_blob.upload_from_string.side_effect = Forbidden("No permission")
        mock_extractor.bucket.blob.return_value = mock_blob
        
        # Execute
        gcs_uri = mock_extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Assert
        assert gcs_uri is None


class TestRun:
    """Tests for the main run pipeline"""
    
    def test_run_success_all_symbols(self, mock_extractor, sample_symbols, sample_price_data):
        """Should process all symbols successfully"""
        # Mock get_symbols_to_process
        mock_extractor.get_symbols_to_process = Mock(return_value=sample_symbols)
        
        # Mock extract_symbol to succeed
        mock_extractor.extract_symbol = Mock(
            return_value="gs://test-bucket/raw/daily/alphavantage/US_NASDAQ_AAPL/2025-10-22.csv"
        )
        
        # Mock manifest write
        mock_extractor._write_manifest = Mock()
        
        # Execute
        exit_code = mock_extractor.run()
        
        # Assert
        assert exit_code == 0
        assert mock_extractor.extract_symbol.call_count == 3
        
        # Verify manifest was written
        mock_extractor._write_manifest.assert_called_once()
        manifest = mock_extractor._write_manifest.call_args[0][0]
        assert len(manifest["success"]) == 3
        assert len(manifest["failed"]) == 0
    
    def test_run_partial_failure(self, mock_extractor):
        """Should handle partial failures and still return success"""
        # Mock symbols
        mock_extractor.get_symbols_to_process = Mock(return_value=[
            ("AAPL", "US", "NASDAQ"),
            ("INVALID", "US", "NASDAQ"),
            ("GOOGL", "US", "NASDAQ")
        ])
        
        # Mock extract_symbol: success, fail, success
        mock_extractor.extract_symbol = Mock(
            side_effect=[
                "gs://bucket/file1.csv",
                None,  # Failure
                "gs://bucket/file2.csv"
            ]
        )
        
        mock_extractor._write_manifest = Mock()
        
        # Execute
        exit_code = mock_extractor.run()
        
        # Assert - should still return 0 if at least one succeeded
        assert exit_code == 0
        
        # Check manifest
        manifest = mock_extractor._write_manifest.call_args[0][0]
        assert len(manifest["success"]) == 2
        assert len(manifest["failed"]) == 1
        assert manifest["failed"][0]["symbol"] == "INVALID"
    
    def test_run_all_failures(self, mock_extractor):
        """Should return error code if all symbols fail"""
        # Mock symbols
        mock_extractor.get_symbols_to_process = Mock(return_value=[
            ("AAPL", "US", "NASDAQ"),
        ])
        
        # Mock extract_symbol to always fail
        mock_extractor.extract_symbol = Mock(return_value=None)
        mock_extractor._write_manifest = Mock()
        
        # Execute
        exit_code = mock_extractor.run()
        
        # Assert
        assert exit_code == 1
    
    def test_run_no_symbols(self, mock_extractor):
        """Should return error if no symbols to process"""
        # Mock empty symbols
        mock_extractor.get_symbols_to_process = Mock(return_value=[])
        mock_extractor._write_manifest = Mock()
        
        # Execute
        exit_code = mock_extractor.run()
        
        # Assert
        assert exit_code == 1


class TestWriteManifest:
    """Tests for manifest writing"""
    
    def test_write_manifest_success(self, mock_extractor):
        """Should write manifest to GCS"""
        # Mock blob
        mock_blob = Mock()
        mock_extractor.bucket.blob.return_value = mock_blob
        
        # Test data
        results = {
            "success": [{"symbol": "AAPL", "gcs_uri": "gs://..."}],
            "failed": [],
            "total": 1
        }
        
        # Execute
        mock_extractor._write_manifest(results)
        
        # Assert
        mock_blob.upload_from_string.assert_called_once()
        manifest_str = mock_blob.upload_from_string.call_args[0][0]
        
        # Verify manifest structure
        import json
        manifest = json.loads(manifest_str)
        assert "run_date" in manifest
        assert "mode" in manifest
        assert "extracted_at" in manifest
        assert "results" in manifest
        assert manifest["results"]["total"] == 1


# Integration-style tests (closer to real behavior)
class TestIntegration:
    """Integration tests with less mocking"""
    
    @patch('alphavantage_extractor.main.storage.Client')
    @patch('alphavantage_extractor.main.bigquery.Client')
    @patch('alphavantage_extractor.main.AlphaVantageClient')
    def test_full_pipeline_flow(self, mock_av_client, mock_bq, mock_storage, mock_env_vars):
        """Test the full flow from start to finish"""
        # Setup mocks
        mock_av = mock_av_client.return_value
        mock_av.get_short_backfill.return_value = pd.DataFrame({
            'timestamp': ['2025-10-22'],
            'open': [150.0],
            'high': [152.0],
            'low': [149.0],
            'close': [151.0],
            'volume': [1000000]
        })
        
        mock_bq_instance = mock_bq.return_value
        mock_result = Mock()
        mock_result.result.return_value = [
            Mock(ticker_symbol="AAPL", exchange_country="US", exchange_code="NASDAQ")
        ]
        mock_bq_instance.query.return_value = mock_result
        
        mock_bucket = mock_storage.return_value.bucket.return_value
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        
        # Execute
        extractor = AlphaVantageExtractor()
        exit_code = extractor.run()
        
        # Assert pipeline completed
        assert exit_code == 0
        
        # Verify all steps were called
        mock_bq_instance.query.assert_called_once()
        mock_av.get_short_backfill.assert_called_with("AAPL")
        assert mock_blob.upload_from_string.call_count == 2  # data + manifest


# How to run these tests:
"""
# Install test dependencies first:
pip install pytest pytest-cov pytest-mock

# Run all tests:
pytest tests/test_extractor.py -v

# Run with coverage:
pytest tests/test_extractor.py --cov=alphavantage_extractor --cov-report=html

# Run specific test class:
pytest tests/test_extractor.py::TestExtractSymbol -v

# Run specific test:
pytest tests/test_extractor.py::TestExtractSymbol::test_extract_symbol_success -v

# Run with print statements visible:
pytest tests/test_extractor.py -v -s
"""