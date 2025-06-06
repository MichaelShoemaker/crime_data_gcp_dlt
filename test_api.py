import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Constants
CHICAGO_CRIME_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
BATCH_SIZE = 100000  # Small batch for testing

def get_api_headers():
    """
    Get API headers with Socrata App Token from environment variable
    """
    app_token = os.getenv('CHICAGO_DATA_PORTAL_TOKEN')
    if not app_token:
        raise ValueError("CHICAGO_DATA_PORTAL_TOKEN not found in .env file")
    
    return {
        'X-App-Token': app_token,
        'Content-Type': 'application/json'
    }

def process_record(record):
    """
    Process a single record to convert dates
    """
    if 'date' in record:
        record['date'] = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S.%f')
    if 'updated_on' in record:
        record['updated_on'] = datetime.strptime(record['updated_on'], '%Y-%m-%dT%H:%M:%S.%f')
    return record

def main():
    try:
        print("Fetching crime data...")
        params = {
            '$limit': BATCH_SIZE,
            '$offset': 0,
            '$order': 'updated_on DESC'
        }
        
        response = requests.get(
            CHICAGO_CRIME_API_URL, 
            params=params,
            headers=get_api_headers()
        )
        response.raise_for_status()
        
        records = response.json()
        print(f"\nRetrieved {len(records)} records")
        
        # Process and print first record as example
        if records:
            first_record = process_record(records[0])
            print("\nExample record:")
            print(json.dumps(first_record, indent=2, default=str))
            
            # Print total count from response headers
            total_count = response.headers.get('X-Total-Count')
            if total_count:
                print(f"\nTotal records available: {total_count}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 