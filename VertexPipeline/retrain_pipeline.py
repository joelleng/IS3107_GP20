import os
from kfp import compiler
from kfp.dsl import pipeline
from google.cloud import aiplatform

from data_processing import data_processing_direct
from train_model import train_model_direct
from evaluate_model import evaluate_model_direct

# UTF-8 to avoid issues
os.environ["PYTHONIOENCODING"] = "utf-8"

# GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../Airflow/keys/sa.json'


# Vertex AI init
aiplatform.init(
    project="is3107-453814",
    location="us-central1"
)

@pipeline(
    name="car_price_retrain",
    pipeline_root="gs://is3107-bucket/mlops/pipeline-artifacts"
)
def retrain_pipeline():
    # 1. Data processing
    processing = data_processing_direct()

    # 2. Train model
    train = train_model_direct().after(processing)

    # 3. Evaluate model
    evaluate_model_direct(
        model_path=train.outputs['model_path']
    ).after(train)

if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=retrain_pipeline,
        package_path="retrain_pipeline.json"
    )

    job = aiplatform.PipelineJob(
        display_name="car_price_pipeline",
        template_path="retrain_pipeline.json",
        pipeline_root="gs://is3107-bucket/mlops/pipeline-artifacts"
    )

    job.run(
        service_account="airflow-service-account@is3107-453814.iam.gserviceaccount.com"
    )
