from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    RunPipelineJobOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

PROJECT_ID = "is3107-453814"
REGION = "us-central1"
TEMPLATE_PATH = "gs://is3107-bucket/mlops/retrain_pipeline.json"

with models.DAG(
    dag_id="trigger_vertex_pipeline_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["vertex", "car-price"],
) as dag:

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_sgcarmart_etl",   
        external_dag_id="SGCarMart_ETL_DAG",            
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=60 * 60,  
        poke_interval=30,  
        mode="poke",
        dag=dag,
    )


    run_pipeline_job = RunPipelineJobOperator(
        task_id="run_vertex_pipeline",
        display_name="trigger-car-price-pipeline",
        template_path=TEMPLATE_PATH,
        region=REGION,
        project_id=PROJECT_ID,
        service_account="airflow-service-account@is3107-453814.iam.gserviceaccount.com",
    )

    wait_for_etl >> run_pipeline_job