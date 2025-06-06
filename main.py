import dlt
import requests
import json
from datetime import datetime, UTC, timedelta
from typing import Dict, Any, Iterator
import functions_framework
from google.cloud import secretmanager
import os
import logging
import gc

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CHICAGO_CRIME_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
BATCH_SIZE = 50  # Reduced batch size for lower memory usage
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
SECRET_ID = 'chicago-data-portal-token'  # Name of your secret in Secret Manager (stores the App Token)

def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Access the secret version from Secret Manager
    """
    try:
        logger.info(f"Accessing secret {secret_id} in project {project_id}")
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret: {str(e)}")
        raise

def get_api_headers() -> Dict[str, str]:
    """
    Get API headers with Socrata App Token from Secret Manager.
    Only the App Token (X-App-Token) is required for Socrata API authentication.
    """
    if not PROJECT_ID:
        error_msg = "GOOGLE_CLOUD_PROJECT environment variable is not set"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        logger.info("Retrieving App Token from Secret Manager")
        app_token = access_secret_version(PROJECT_ID, SECRET_ID)
        return {
            'X-App-Token': app_token,
            'Content-Type': 'application/json'
        }
    except Exception as e:
        error_msg = f"Failed to retrieve App Token from Secret Manager: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

def process_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single record to convert dates and add load ID
    """
    try:
        if 'date' in record:
            record['date'] = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S.%f')
        if 'updated_on' in record:
            record['updated_on'] = datetime.strptime(record['updated_on'], '%Y-%m-%dT%H:%M:%S.%f')
        
        record['_dlt_load_id'] = datetime.now(UTC).isoformat()
        return record
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        raise

@dlt.resource(
    table_name="crimes",
    write_disposition="merge",
    primary_key="id",
    merge_key="id"
)
def fetch_crime_data(offset: int = 0, limit: int = BATCH_SIZE) -> Iterator[Dict[str, Any]]:
    """
    Fetch crime data from Chicago Data Portal API with pagination
    """
    try:
        # Set the date filter to January 1st, 2025
        start_date = "2025-01-01T00:00:00.000"
        
        logger.info(f"Fetching crime data from {start_date}")
        params = {
            '$limit': limit,
            '$offset': offset,
            '$order': 'date DESC',  # Order by date to get most recent records first
            '$where': f"date >= '{start_date}'"  # Filter for records from 2025 onwards
        }
        
        response = requests.get(
            CHICAGO_CRIME_API_URL, 
            params=params,
            headers=get_api_headers()
        )
        response.raise_for_status()
        
        # Process records one at a time to reduce memory usage
        for record in response.json():
            yield process_record(record)
    except Exception as e:
        logger.error(f"Error fetching crime data: {str(e)}")
        raise

@functions_framework.http
def crime_data_loader(request):
    """
    Cloud Function entry point
    """
    try:
        logger.info("Starting crime data loader function")
        logger.info(f"Project ID: {PROJECT_ID}")
        
        # Initialize DLT pipeline with BigQuery destination
        pipeline = dlt.pipeline(
            pipeline_name='chicago_crime_v2',  # New pipeline name to start fresh
            destination='bigquery',
            dataset_name='crime_data'
        )

        offset = 0
        total_records = 0
        last_load_info = None
        
        while True:
            # Process records in smaller batches
            batch = []
            batch_size = 0
            
            for record in fetch_crime_data(offset=offset):
                batch.append(record)
                batch_size += 1
                if batch_size >= BATCH_SIZE:
                    break
            
            if not batch:
                logger.info("No more records to process")
                break
                
            # Load data using DLT
            logger.info(f"Loading batch of {len(batch)} records")
            try:
                # Load the current batch
                last_load_info = pipeline.run(
                    batch,
                    table_name='crimes'
                )
                total_records += len(batch)
                offset += len(batch)
                
                # Force garbage collection after each batch
                del batch
                gc.collect()
                
                # If we got fewer records than the batch size, we've reached the end
                if batch_size < BATCH_SIZE:
                    logger.info("Reached end of data")
                    break
            except Exception as e:
                logger.error(f"Error loading batch: {str(e)}")
                raise

        logger.info(f"Successfully processed {total_records} records")
        return {
            'status': 'success',
            'message': f'Successfully processed {total_records} records',
            'load_info': last_load_info
        }

    except Exception as e:
        error_msg = f"Error in crime_data_loader: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'error',
            'message': error_msg
        }, 500

if __name__ == "__main__":
    # For local testing
    crime_data_loader(None) 