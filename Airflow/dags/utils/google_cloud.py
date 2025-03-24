from google.cloud import bigquery, storage
from datetime import datetime
import io
import pandas as pd

# ----------------------------- Configurable Variables -----------------------------
GCP_PROJECT_ID = 'is3107-453814'
BQ_DATASET_ID = 'car_dataset'
BQ_TABLE_ID = 'coe_bidding_results'
BUCKET_NAME = 'is3107-bucket'

def upload_to_gcs(df, bucket_name=BUCKET_NAME, prefix=BQ_TABLE_ID):
    """Uploads transformed data to Google Cloud Storage."""
    if df.empty:
        print("⚠️ No new data to upload. Skipping GCS upload.")
        return None

    print("🚀 Uploading transformed data to GCS...")

    # Initialize GCS client and get the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate a timestamp-based blob name to avoid overwriting
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    destination_blob_name = f"{prefix}_{timestamp}.csv"
    blob = bucket.blob(destination_blob_name)

    # Convert the DataFrame to CSV and upload it
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"✅ Data successfully uploaded to {destination_blob_name}")
    return f"gs://{bucket_name}/{destination_blob_name}"

def load_to_bigquery(gcs_uri, gcp_project_id=GCP_PROJECT_ID, bq_dataset_id=BQ_DATASET_ID, bq_table_id=BQ_TABLE_ID):
    """Loads data from Google Cloud Storage into BigQuery."""
    if not gcs_uri:
        print("⚠️ No GCS file to load. Skipping BigQuery load.")
        return

    print(f"📊 Loading data from {gcs_uri} into BigQuery...")

    client = bigquery.Client(project=gcp_project_id)
    table_ref = f"{gcp_project_id}.{bq_dataset_id}.{bq_table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load the CSV data from GCS to BigQuery
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete

    print(f"✅ Data successfully loaded into BigQuery.")
