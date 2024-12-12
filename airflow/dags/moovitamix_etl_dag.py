from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import requests
import json
import os
from pathlib import Path

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def get_api_connection():
    """Get API connection details"""
    conn = BaseHook.get_connection('moovitamix_api')
    base_url = f"{conn.host}"
    if conn.port:
        base_url = f"{base_url}:{conn.port}"
    return base_url

def create_directory(directory_path):
    """Safely create directory and set permissions"""
    try:
        # Create parent directories if they don't exist
        Path(directory_path).parent.mkdir(parents=True, exist_ok=True)
        # Create the final directory
        Path(directory_path).mkdir(exist_ok=True)
        logger.info(f"Successfully created directory: {directory_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory_path}: {str(e)}")
        raise

def extract_data(**context):
    """Extract data from API endpoints"""
    try:
        # Setup directory - using absolute path
        data_dir = os.path.join('/opt/airflow/data/raw', context['ds'])
        create_directory(data_dir)
        logger.info(f"Using data directory: {data_dir}")
        
        # Get API connection
        base_url = get_api_connection()
        logger.info(f"Using API base URL: {base_url}")
        
        # API endpoints
        endpoints = ['tracks', 'users', 'listen_history']
        
        for endpoint in endpoints:
            try:
                # Make API request
                response = requests.get(f"{base_url}/{endpoint}")
                response.raise_for_status()
                data = response.json()
                
                # Save to file
                file_path = os.path.join(data_dir, f"{endpoint}.json")
                logger.info(f"Saving data to: {file_path}")
                
                with open(file_path, 'w') as f:
                    json.dump(data, f)
                
                logger.info(f"Successfully processed {endpoint} data")
                
                # Store metrics
                context['task_instance'].xcom_push(
                    key=f'{endpoint}_count',
                    value=len(data.get('items', []))
                )
                
            except Exception as e:
                logger.error(f"Error processing {endpoint}: {str(e)}")
                raise
        
        return True
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise

def load_data(**context):
    """Load data into database"""
    try:
        data_dir = os.path.join('/opt/airflow/data/raw', context['ds'])
        logger.info(f"Loading data from directory: {data_dir}")
        
        # Verify files exist
        for endpoint in ['tracks', 'users', 'listen_history']:
            file_path = os.path.join(data_dir, f"{endpoint}.json")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Required file not found: {file_path}")
            
            with open(file_path, 'r') as f:
                data = json.load(f)
                logger.info(f"Found {len(data.get('items', []))} records for {endpoint}")
        
        logger.info("Data load completed successfully")
        return True
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        raise

with DAG(
    'moovitamix_etl',
    default_args=default_args,
    description='MooVitamix ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 12),
    catchup=False,
    tags=['moovitamix', 'etl']
) as dag:

    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='moovitamix_api',
        endpoint='health',
        request_params={},
        response_check=lambda response: response.json()["status"] == "healthy",
        poke_interval=30,
        timeout=300,
        mode='poke',
    )

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Set up dependencies
    check_api >> extract >> load