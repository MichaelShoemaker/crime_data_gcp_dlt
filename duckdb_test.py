import dlt
import requests
from typing import Iterator, Dict, Any

# Chicago Crime API endpoint
API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

# Your App Token from Socrata (Chicago Data Portal)
APP_TOKEN = "i1E4GLIddoJIfwcwnIKt9zqEY"

# Batch size per request (Socrata default is 1000 max)
BATCH_SIZE = 500

def get_crime_data() -> Iterator[Dict[str, Any]]:
    offset = 0
    while True:
        params = {
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "date DESC",
            "$where": "date >= '2025-01-01T00:00:00.000'"
        }
        headers = {
            "X-App-Token": APP_TOKEN
        }
        response = requests.get(API_URL, params=params, headers=headers)
        data = response.json()

        if not data:
            break

        for record in data:
            yield record

        offset += BATCH_SIZE

# Define the DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="chicago_crime_test",
    destination="duckdb",  # or "bigquery", "postgres", etc.
    dataset_name="crime_data"
)

# Run the pipeline with the data generator
load_info = pipeline.run(get_crime_data(), table_name="crime")

# Show the result of the pipeline run
print(load_info)
