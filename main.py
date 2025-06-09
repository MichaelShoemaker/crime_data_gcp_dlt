import dlt
import requests
from datetime import datetime, timezone
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
BATCH_SIZE = 100  # Reduced batch size
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
SECRET_ID = 'chicago-data-portal-token'

def get_api_token() -> str:
    """Get API token from Secret Manager"""
    try:
        logger.info("Getting API token from Secret Manager")
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret: {str(e)}")
        raise

def process_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single record"""
    try:
        # Convert date strings to datetime objects
        if 'date' in record:
            record['date'] = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S.%f')
        if 'updated_on' in record:
            record['updated_on'] = datetime.strptime(record['updated_on'], '%Y-%m-%dT%H:%M:%S.%f')
        
        # Add load timestamp
        record['_dlt_load_id'] = datetime.now(timezone.utc).isoformat()
        
        # Ensure ID is a string
        record['id'] = str(record.get('id', ''))
        
        return record
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        raise

@dlt.resource(
    table_name="crimes",
    write_disposition="merge",
    primary_key=["id"],
    merge_key="id"
)
def fetch_crime_data(offset: int = 0) -> Iterator[Dict[str, Any]]:
    """Fetch crime data from Chicago Data Portal API"""
    try:
        # Set date filter
        start_date = "2025-01-01T00:00:00.000"
        
        # Configure API request
        params = {
            '$limit': BATCH_SIZE,
            '$offset': offset,
            '$where': f"date >= '{start_date}'"
        }
        
        logger.info(f"Fetching data with params: {params}")
        
        headers = {
            'X-App-Token': get_api_token(),
            'Content-Type': 'application/json'
        }
        
        # Make API request
        response = requests.get(CHICAGO_CRIME_API_URL, params=params, headers=headers)
        response.raise_for_status()
        
        # Process and yield records
        records = response.json()
        logger.info(f"Received {len(records)} records from API")
        
        for record in records:
            yield process_record(record)
            
    except Exception as e:
        logger.error(f"Error fetching crime data: {str(e)}")
        raise

@functions_framework.http
def crime_data_loader(request):
    """Cloud Function entry point"""
    try:
        logger.info("Starting crime data loader")
        
        # Initialize pipeline
        pipeline = dlt.pipeline(
            pipeline_name='chicago_crime',
            destination='bigquery',
            dataset_name='crime_data'
        )

        offset = 0
        total_records = 0
        max_batches = 5  # Limit number of batches per function call
        current_batch = 0
        
        while current_batch < max_batches:
            logger.info(f"Processing batch {current_batch + 1} of {max_batches} (offset: {offset})")
            
            # Fetch and process batch
            batch = list(fetch_crime_data(offset=offset))
            
            if not batch:
                logger.info("No more records to process")
                break
            
            # Load batch
            logger.info(f"Loading batch of {len(batch)} records")
            pipeline.run(batch, table_name='crimes')
            
            # Update counters
            total_records += len(batch)
            offset += len(batch)
            current_batch += 1
            
            # Check if we got a partial batch (end of data)
            is_partial = len(batch) < BATCH_SIZE
            
            # Clean up
            del batch
            gc.collect()
            
            # Break if we got a partial batch
            if is_partial:
                logger.info("Reached end of data")
                break
        
        logger.info(f"Successfully processed {total_records} records")
        return {
            'status': 'success',
            'records_processed': total_records,
            'next_offset': offset,
            'has_more': current_batch >= max_batches and not is_partial
        }

    except Exception as e:
        error_msg = f"Error: {str(e)}"
        logger.error(error_msg)
        return {'status': 'error', 'message': error_msg}, 500

if __name__ == "__main__":
    crime_data_loader(None) 