from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from google.cloud import bigquery
from datetime import datetime, timezone
from utils import google_cloud

# ----------------------------- Google Cloud -----------------------------
GCP_PROJECT_ID = 'is3107-453814'
BQ_DATASET_ID = 'car_dataset'
BQ_TABLE_ID = 'coe_price'
BUCKET_NAME = 'is3107-bucket'

# ----------------------------- COE API -----------------------------
RESOURCE_ID = 'd_69b3380ad7e51aff3a7dcc84eba52b8a'
BASE_URL = 'https://data.gov.sg/api/action/datastore_search'
LIMIT = 100

# ----------------------------- DAG -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'catchup': False,
}

@dag(
    schedule_interval='@weekly',
    default_args=default_args,
    tags=['bigquery', 'gcs', 'etl']
)
def coe_etl_pipeline():
    @task
    def check_or_create_table():
        """Check BigQuery for the table and create it if it doesn't exist with the new schema."""
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        # Not necessary and just a contingency
        schema = [
            bigquery.SchemaField("coe_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("month", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("bidding_no", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("vehicle_class", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("quota", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("bids_success", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("bids_received", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("premium", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "DATETIME", mode="NULLABLE"),
        ]

        try:
            client.get_table(table_id)
            print(f"✅ Table {table_id} already exists.")
        except Exception as e:
            if "Not found" in str(e):
                print(f"🚀 Creating table {table_id}...")
                table = bigquery.Table(table_id, schema=schema)
                client.create_table(table)
            else:
                raise e

    @task
    def get_latest_offset():
        """Retrieve the latest `coe_id` from BigQuery to determine where to start fetching data."""
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        query = f"SELECT MAX(coe_id) AS latest_id FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.result()
        latest_id = next(result).latest_id

        return 0 if latest_id is None else latest_id + 1

    @task
    def extract_data(offset):
        """Extract records from the API using dynamic offset."""
        all_records = []
        while True:
            params = {'resource_id': RESOURCE_ID, 'limit': LIMIT, 'offset': offset}
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            records = response.json().get('result', {}).get('records', [])

            if not records:
                break  # No more records

            all_records.extend(records)
            offset += LIMIT

        return all_records

    @task
    def transform_data(records):
        """Transform the data, rename fields, convert `month` to DATE, and add timestamp."""
        if not records:
            return None  # No data to process

        df = pd.DataFrame(records)

        # Rename _id to coe_id
        df.rename(columns={'_id': 'coe_id'}, inplace=True)

        # Convert 'month' (YYYY-MM) into a proper DATE format (YYYY-MM-01)
        if 'month' in df.columns:
            df['month'] = pd.to_datetime(df['month'] + '-01').dt.date  # Keep only date part

        df['timestamp'] = datetime.now(timezone.utc).replace(tzinfo=None)

        return df

    @task
    def upload_to_gcs(df):
        """Uploads DataFrame to GCS via google_cloud.py"""
        return google_cloud.upload_to_gcs(df, bucket_name=BUCKET_NAME)

    @task
    def load_to_bigquery(gcs_uri):
        """Loads GCS data into BigQuery via google_cloud.py"""
        google_cloud.load_to_bigquery(gcs_uri, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    # DAG Task Dependencies
    check_or_create_table()
    latest_offset = get_latest_offset()
    raw_data = extract_data(latest_offset)
    transformed_data = transform_data(raw_data)
    gcs_uri = upload_to_gcs(transformed_data)
    load_to_bigquery(gcs_uri)

coe_etl_dag = coe_etl_pipeline()
