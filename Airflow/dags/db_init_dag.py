from airflow.decorators import dag, task
from google.cloud import bigquery
from datetime import datetime
from utils import schemas  

GCP_PROJECT_ID = "is3107-453814"

# Define default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
}

@dag(
    default_args=default_args,
    schedule=None,  # Run once
    tags=["bigquery", "schema"],
)
def create_bigquery_tables():
    @task
    def create_tables():
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        for table_id, schema in schemas.TABLE_SCHEMAS.items():
            dataset_id, table_name = table_id.split(".")
            table_ref = client.dataset(dataset_id).table(table_name)

            try:
                # Check if table exists
                client.get_table(table_ref)
                print(f"Table {table_id} already exists. Skipping creation.")
            except Exception as e:
                if "Not found" in str(e):
                    # Table does not exist, so create it
                    table = bigquery.Table(table_ref, schema=schema)
                    client.create_table(table)
                    print(f"Created table {table_id}")
                else:
                    raise e

    create_tables()

dag_instance = create_bigquery_tables()
