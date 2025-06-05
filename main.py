import dlt
import requests
import json
from datetime import datetime
from typing import Dict, Any, Iterator
import functions_framework
from google.cloud import storage
from google.cloud import secretmanager
import os

# Constants
CHICAGO_CRIME_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
BATCH_SIZE = 1000  # Number of records per API call
BUCKET_NAME = "your-bucket-name"  # Replace with your GCS bucket name
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
SECRET_ID = 'chicago-data-portal-token'  # Name of your secret in Secret Manager

def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Access the secret version from Secret Manager
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_api_headers() -> Dict[str, str]:
    """
    Get API headers with authentication token from Secret Manager
    """
    if not PROJECT_ID:
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is not set")
    
    try:
        api_token = access_secret_version(PROJECT_ID, SECRET_ID)
        return {
            'X-App-Token': api_token,
            'Content-Type': 'application/json'
        }
    except Exception as e:
        raise ValueError(f"Failed to retrieve API token from Secret Manager: {str(e)}")

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
    params = {
        '$limit': limit,
        '$offset': offset,
        '$order': 'updated_on DESC'  # Order by updated_on to get most recent changes first
    }
    
    response = requests.get(
        CHICAGO_CRIME_API_URL, 
        params=params,
        headers=get_api_headers()
    )
    response.raise_for_status()
    data = response.json()
    
    for record in data:
        # Convert date strings to datetime objects
        if 'date' in record:
            record['date'] = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S.%f')
        if 'updated_on' in record:
            record['updated_on'] = datetime.strptime(record['updated_on'], '%Y-%m-%dT%H:%M:%S.%f')
        
        # Add a timestamp for when we processed this record
        record['_dlt_load_id'] = datetime.utcnow().isoformat()
        yield record

@functions_framework.http
def crime_data_loader(request):
    """
    Cloud Function entry point
    """
    try:
        # Initialize DLT pipeline with BigQuery destination
        pipeline = dlt.pipeline(
            pipeline_name='chicago_crime',
            destination='bigquery',
            dataset_name='crime_data'
        )

        offset = 0
        total_records = 0
        
        while True:
            # Fetch and process batch of data using the resource
            batch = list(fetch_crime_data(offset=offset))
            
            if not batch:
                break
                
            # Load data using DLT
            load_info = pipeline.run(
                batch,
                table_name='crimes'
            )
            
            total_records += len(batch)
            offset += len(batch)
            
            # If we got fewer records than the batch size, we've reached the end
            if len(batch) < BATCH_SIZE:
                break

        return {
            'status': 'success',
            'message': f'Successfully processed {total_records} records',
            'load_info': load_info
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }, 500

if __name__ == "__main__":
    # For local testing
    crime_data_loader(None) 