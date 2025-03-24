from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from utils import google_cloud  # Import your helper functions
import pandas as pd
import requests
from google.cloud import bigquery, storage

# ----------------------------- Google Cloud -----------------------------
GCP_PROJECT_ID = 'is3107-453814'
BQ_DATASET_ID = 'car_dataset'
BQ_TABLE_ID = 'coe_bidding_results'
BUCKET_NAME = 'is3107-bucket'

# ----------------------------- COE -----------------------------
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
    def get_latest_offset():
        """Check BigQuery for the latest record ID to determine offset, 
        and create the table if it doesn't exist."""
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        
        # Try to retrieve the table
        try:
            table = client.get_table(table_id)
        except Exception as e:
            # If the table doesn't exist, define the schema and create it
            print("Table not found, creating new table with defined schema...")
            schema = [
                bigquery.SchemaField("_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("month", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bidding_no", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("vehicle_class", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quota", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("bids_success", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("bids_received", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("premium", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("month_num", "INTEGER", mode="NULLABLE"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)
            # Table created so no records exist; start offset from 0.
            return 0

        # If the table exists, query for the maximum _id
        query = f"SELECT MAX(_id) AS latest_id FROM {table_id}"
        query_job = client.query(query)
        result = query_job.result()
        latest_id = next(result).latest_id

        if latest_id is None:
            return 0  # Table is empty, start from 0
        return latest_id + 1

    @task()
    def extract_data(offset):
        """Extract records from API using dynamic offset."""
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

    @task()
    def transform_data(records):
        """Transforms data and prepares for storage."""
        df = pd.DataFrame(records)
        if 'month' in df.columns:
            df[['year', 'month_num']] = df['month'].str.split('-', expand=True).astype(int)
        return df

    @task()
    def upload_to_gcs(df):
        """Uploads DataFrame to GCS via google_cloud.py"""
        return google_cloud.upload_to_gcs(df, bucket_name=BUCKET_NAME)

    @task()
    def load_to_bigquery(gcs_uri):
        """Loads GCS data into BigQuery via google_cloud.py"""
        google_cloud.load_to_bigquery(gcs_uri, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)

    # DAG Task Dependencies
    latest_offset = get_latest_offset()
    raw_data = extract_data(latest_offset)
    transformed_data = transform_data(raw_data)
    gcs_uri = upload_to_gcs(transformed_data)
    load_to_bigquery(gcs_uri)

coe_etl_dag = coe_etl_pipeline()
