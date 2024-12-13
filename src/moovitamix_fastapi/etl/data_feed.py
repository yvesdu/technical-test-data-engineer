import os
import logging
from datetime import datetime
import requests
import pandas as pd
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MooVitamixDataFeed:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.endpoints = {
            'tracks': '/tracks',
            'users': '/users',
            'listen_history': '/listen_history'
        }
        self.output_dir = os.path.join('data', 'raw', datetime.now().strftime('%Y-%m-%d'))

    def _make_request(self, endpoint: str) -> List[Dict[Any, Any]]:
        """Make paginated requests to the API endpoint"""
        url = f"{self.base_url}{endpoint}"
        all_items = []
        page = 1
        
        while True:
            try:
                response = requests.get(f"{url}?page={page}&size=100")
                response.raise_for_status()
                data = response.json()
                
                if not data['items']:
                    break
                    
                all_items.extend(data['items'])
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from {url}: {str(e)}")
                raise
                
        return all_items

    def _save_to_parquet(self, data: List[Dict], filename: str):
        """Save the data to a parquet file"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        df = pd.DataFrame(data)
        output_path = os.path.join(self.output_dir, f"{filename}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(data)} records to {output_path}")

    def extract_all(self):
        """Extract data from all endpoints"""
        try:
            for name, endpoint in self.endpoints.items():
                logger.info(f"Extracting data from {endpoint}")
                data = self._make_request(endpoint)
                self._save_to_parquet(data, name)
                logger.info(f"Successfully extracted {name} data")
                
        except Exception as e:
            logger.error(f"Error in extract_all: {str(e)}")
            raise

def main():
    try:
        data_feed = MooVitamixDataFeed()
        data_feed.extract_all()
        logger.info("Data extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())