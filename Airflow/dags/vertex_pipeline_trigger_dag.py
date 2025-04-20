from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai import CreatePipelineJobOperator
from datetime import datetime

PROJECT_ID = "is3107-453814"
REGION = "us-central1"
PIPELINE_NAME = "car-price-retrain-pipeline"
PIPELINE_ROOT = "gs://is3107-car-data/pipeline-artifacts"
TEMPLATE_PATH = f"{PIPELINE_ROOT}/retrain_pipeline.json"

with models.DAG(
    dag_id="trigger_vertex_pipeline_dag",
    schedule_interval=None,  # or '0 9 * * *' for daily at 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["vertex", "car-price"],
) as dag:

    trigger_pipeline = CreatePipelineJobOperator(
        task_id="run_vertex_pipeline",
        project_id=PROJECT_ID,
        region=REGION,
        display_name="trigger-car-price-pipeline",
        template_path=TEMPLATE_PATH,
        pipeline_root=PIPELINE_ROOT,
    )