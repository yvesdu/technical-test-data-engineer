import os
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from moovitamix_fastapi.etl.data_feed import DataFeed

@pytest.fixture
def data_feed():
    return DataFeed(base_url="http://test-api")

@pytest.fixture
def mock_response():
    mock = Mock()
    mock.json.return_value = {
        "items": [
            {"id": 1, "name": "Test Track", "artist": "Test Artist"},
            {"id": 2, "name": "Test Track 2", "artist": "Test Artist 2"}
        ]
    }
    return mock

def test_make_request_successful(data_feed, mock_response):
    """Test successful API request with pagination"""
    with patch('requests.get') as mock_get:
        # First page has data, second page empty (end of pagination)
        mock_get.side_effect = [
            mock_response,
            Mock(json=lambda: {"items": []})
        ]
        
        result = data_feed._make_request('/tracks')
        
        assert len(result) == 2
        assert result[0]['id'] == 1
        assert result[1]['name'] == "Test Track 2"
        assert mock_get.call_count == 2

def test_make_request_handles_error(data_feed):
    """Test error handling in API request"""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API Error")
        
        with pytest.raises(Exception) as exc_info:
            data_feed._make_request('/tracks')
        
        assert "API Error" in str(exc_info.value)

def test_save_to_parquet(data_feed, tmp_path):
    """Test saving data to parquet file"""
    # Override output directory for testing
    data_feed.output_dir = str(tmp_path)
    
    test_data = [
        {"id": 1, "name": "Test Track", "artist": "Test Artist"},
        {"id": 2, "name": "Test Track 2", "artist": "Test Artist 2"}
    ]
    
    data_feed._save_to_parquet(test_data, "test_tracks")
    
    # Check if file exists
    expected_path = os.path.join(str(tmp_path), "test_tracks.parquet")
    assert os.path.exists(expected_path)
    
    # Verify data integrity
    df = pd.read_parquet(expected_path)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name", "artist"]
    assert df.iloc[0]["name"] == "Test Track"

def test_extract_all_integration(data_feed, mock_response, tmp_path):
    """Integration test for the full extraction process"""
    # Override output directory for testing
    data_feed.output_dir = str(tmp_path)
    
    with patch('requests.get') as mock_get:
        # Mock successful responses for all endpoints
        mock_get.side_effect = [
            mock_response,  # tracks first page
            Mock(json=lambda: {"items": []}),  # tracks second page
            mock_response,  # users first page
            Mock(json=lambda: {"items": []}),  # users second page
            mock_response,  # listen_history first page
            Mock(json=lambda: {"items": []})   # listen_history second page
        ]
        
        data_feed.extract_all()
        
        # Verify all files were created
        assert os.path.exists(os.path.join(str(tmp_path), "tracks.parquet"))
        assert os.path.exists(os.path.join(str(tmp_path), "users.parquet"))
        assert os.path.exists(os.path.join(str(tmp_path), "listen_history.parquet"))
        
        # Verify mock calls
        assert mock_get.call_count == 6  # Two calls per endpoint (pagination)