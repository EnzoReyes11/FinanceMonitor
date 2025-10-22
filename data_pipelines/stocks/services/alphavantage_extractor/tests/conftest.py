# ===== conftest.py (shared fixtures) =====
# Put this in tests/conftest.py

"""
Shared test fixtures and configuration.
This file is automatically loaded by pytest.
"""

import pytest
import os
from unittest.mock import Mock
import pandas as pd


@pytest.fixture(autouse=True)
def reset_env():
    """Reset environment variables between tests"""
    original_env = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_dataframe():
    """Reusable sample DataFrame"""
    return pd.DataFrame({
        'timestamp': ['2025-10-22', '2025-10-21'],
        'open': [150.0, 148.0],
        'high': [152.0, 150.0],
        'low': [149.0, 147.0],
        'close': [151.5, 149.5],
        'volume': [1000000, 950000]
    })


@pytest.fixture
def mock_logger():
    """Mock logger for testing log output"""
    return Mock()


# Custom markers (for categorizing tests)
def pytest_configure(config):
    config.addinivalue_line("markers", "unit: Unit tests (fast, heavily mocked)")
    config.addinivalue_line("markers", "integration: Integration tests (slower, less mocked)")
    config.addinivalue_line("markers", "slow: Tests that take a long time")
    config.addinivalue_line("markers", "requires_gcp: Tests that need GCP credentials")


# ===== Example: tests/test_extractor_simple.py =====
# A simpler test file focusing on key functionality

"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from alphavantage_extractor.main import AlphaVantageExtractor


@pytest.mark.unit
def test_extract_adds_metadata_columns(monkeypatch):
    '''Test that extract_symbol adds required metadata columns'''
    # Setup
    monkeypatch.setenv("ALPHA_VANTAGE_API_TOKEN", "test")
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    
    with patch('alphavantage_extractor.main.AlphaVantageClient'), \
         patch('alphavantage_extractor.main.storage.Client'), \
         patch('alphavantage_extractor.main.bigquery.Client'):
        
        extractor = AlphaVantageExtractor()
        
        # Mock API response
        mock_df = pd.DataFrame({'timestamp': ['2025-10-22'], 'close': [150.0]})
        extractor.client.get_short_backfill = Mock(return_value=mock_df)
        
        # Mock GCS
        mock_blob = Mock()
        extractor.bucket.blob = Mock(return_value=mock_blob)
        
        # Execute
        gcs_uri = extractor.extract_symbol("AAPL", "US", "NASDAQ")
        
        # Get uploaded data
        uploaded_csv = mock_blob.upload_from_string.call_args[0][0]
        
        # Verify metadata columns were added
        assert 'ticker_symbol,AAPL' in uploaded_csv or 'ticker_symbol\\nAAPL' in uploaded_csv
        assert 'exchange_name' in uploaded_csv
        assert 'country' in uploaded_csv
        assert 'is_adjusted' in uploaded_csv


@pytest.mark.unit  
def test_manifest_contains_required_fields(monkeypatch):
    '''Test that manifest has all required fields'''
    monkeypatch.setenv("ALPHA_VANTAGE_API_TOKEN", "test")
    
    with patch('alphavantage_extractor.main.AlphaVantageClient'), \
         patch('alphavantage_extractor.main.storage.Client'), \
         patch('alphavantage_extractor.main.bigquery.Client'):
        
        extractor = AlphaVantageExtractor()
        
        mock_blob = Mock()
        extractor.bucket.blob = Mock(return_value=mock_blob)
        
        # Execute
        results = {
            "success": [{"symbol": "AAPL", "gcs_uri": "gs://..."}],
            "failed": [],
            "total": 1
        }
        extractor._write_manifest(results)
        
        # Get manifest JSON
        import json
        manifest_str = mock_blob.upload_from_string.call_args[0][0]
        manifest = json.loads(manifest_str)
        
        # Verify structure
        assert "run_date" in manifest
        assert "mode" in manifest
        assert "extracted_at" in manifest
        assert "results" in manifest
"""