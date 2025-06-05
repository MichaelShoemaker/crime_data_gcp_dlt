# Chicago Crime Data Pipeline

This Cloud Function fetches crime data from the Chicago Data Portal API and loads it into BigQuery using DLT (Data Load Tool). It handles incremental updates by using the `updated_on` field and performs upserts based on the record `id`.

## Prerequisites

- Google Cloud Platform account
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed and configured
- Access to the Chicago Data Portal API (get your App Token from [data.cityofchicago.org](https://data.cityofchicago.org/))

## Setup Instructions

### 1. Set Your Project ID

First, list your available projects:
```bash
gcloud projects list
```

Then set your project ID (replace with your actual project ID):
```bash
# Set your Google Cloud project ID
export GOOGLE_CLOUD_PROJECT="your-project-id"

# Verify the project is set
echo $GOOGLE_CLOUD_PROJECT

# Set the project in gcloud
gcloud config set project $GOOGLE_CLOUD_PROJECT

# Verify the project is set correctly
gcloud config get-value project
```

### 2. Create a Secret for the App Token

The Chicago Data Portal uses the Socrata API, which only requires an App Token (X-App-Token) for authentication. You can get your App Token by:
1. Going to [data.cityofchicago.org](https://data.cityofchicago.org/)
2. Creating an account or signing in
3. Going to your profile settings
4. Generating an App Token

Then store it in Secret Manager:
```bash
# Create a secret in Secret Manager
gcloud secrets create chicago-data-portal-token --replication-policy="automatic"

# Add your App Token as a version
echo -n "your-app-token-here" | gcloud secrets versions add chicago-data-portal-token --data-file=-

# Get your project number (make sure GOOGLE_CLOUD_PROJECT is set first)
PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format='value(projectNumber)')
echo "Project Number: $PROJECT_NUMBER"

# Grant access to the Cloud Function's service account
gcloud secrets add-iam-policy-binding chicago-data-portal-token \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

### 3. Create BigQuery Dataset

```bash
# Create the dataset in BigQuery
bq mk --dataset \
    --description "Chicago Crime Data" \
    $GOOGLE_CLOUD_PROJECT:crime_data
```

### 4. Deploy the Cloud Function

Choose one of these regions (replace REGION with your choice):
- us-central1 (Iowa)
- us-east1 (South Carolina)
- us-east4 (Northern Virginia)
- us-west1 (Oregon)
- us-west2 (Los Angeles)
- us-west3 (Salt Lake City)
- us-west4 (Las Vegas)

```bash
# Deploy the function with authentication and memory settings
gcloud functions deploy crime_data_loader \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=crime_data_loader \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --memory=256MB \
  --timeout=540s \
  --set-env-vars GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}"
```

### 5. Set Up Cloud Scheduler

After deploying the function, you'll need the function's URL. You can get it with:
```bash
# Get the function URL (make sure to use the same region as your deployment)
FUNCTION_URL=$(gcloud functions describe crime_data_loader --gen2 --region=us-central1 --format='value(serviceConfig.uri)')
echo "Function URL: $FUNCTION_URL"
```

#### Schedule Options

1. **Daily at midnight (Chicago time)**:
```bash
gcloud scheduler jobs create http crime-data-daily \
  --schedule="0 0 * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --time-zone="America/Chicago" \
  --oidc-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --location=us-central1
```

2. **Every 6 hours**:
```bash
gcloud scheduler jobs create http crime-data-6hourly \
  --schedule="0 */6 * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --time-zone="America/Chicago" \
  --oidc-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --location=us-central1
```

3. **Every hour**:
```bash
gcloud scheduler jobs create http crime-data-hourly \
  --schedule="0 * * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --time-zone="America/Chicago" \
  --oidc-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --location=us-central1
```

4. **Custom schedule** (e.g., every 30 minutes):
```bash
gcloud scheduler jobs create http crime-data-30min \
  --schedule="*/30 * * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --time-zone="America/Chicago" \
  --oidc-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --location=us-central1
```

#### Managing Scheduler Jobs

To list all scheduler jobs:
```bash
gcloud scheduler jobs list --location=us-central1
```

To delete a scheduler job:
```bash
gcloud scheduler jobs delete JOB_NAME --location=us-central1
```

To update a job's schedule:
```bash
gcloud scheduler jobs update http JOB_NAME --schedule="NEW_SCHEDULE" --location=us-central1
```

## How It Works

1. The function fetches data from the Chicago Data Portal API in batches of 1000 records
2. Records are ordered by `updated_on` timestamp to get the most recent changes first
3. DLT handles the data loading with the following configuration:
   - `write_disposition="merge"` for upserts
   - `primary_key="id"` for uniqueness
   - `merge_key="id"` for matching records to update
4. New records are inserted, existing records are updated only if they've changed
5. Each record includes a `_dlt_load_id` timestamp to track when it was processed

## Local Development

To run the function locally:

```bash
# Set your App Token for local testing
export CHICAGO_DATA_PORTAL_TOKEN="your-app-token-here"

# Run the function
python main.py
```

## Dependencies

The project uses the following main dependencies:
- dlt[bigquery]>=0.3.0
- requests>=2.31.0
- functions-framework>=3.4.0
- google-cloud-bigquery>=3.17.0
- google-cloud-secret-manager>=2.18.0

## Monitoring

You can monitor the function's execution in the Google Cloud Console:
- Cloud Functions > crime_data_loader > Logs
- BigQuery > crime_data dataset > crimes table
- Cloud Scheduler > Jobs > [Your Job Name] > Execution history

The function returns a response with:
- Status of the operation
- Number of records processed
- Load information from DLT 