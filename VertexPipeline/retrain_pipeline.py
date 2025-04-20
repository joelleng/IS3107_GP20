import os
from google.cloud import aiplatform
from kfp import compiler
from kfp.dsl import pipeline

from train_model import train_model_direct
from evaluate_model import evaluate_model_direct

# Force UTF-8 to avoid UnicodeEncodeError
os.environ["PYTHONIOENCODING"] = "utf-8"

# Vertex AI project config
aiplatform.init(
    project="is3107-453814",
    location="us-central1"
)

# Define the actual pipeline
@pipeline(
    name="car_price_retrain_direct",
    pipeline_root="gs://is3107-car-data/pipeline-artifacts"
)
def retrain_pipeline_direct():
    train = train_model_direct()
    evaluate_model_direct(model_path=train.outputs["model_path"])

# Compile and Run
if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=retrain_pipeline_direct,
        package_path="retrain_pipeline.json",
    )

    job = aiplatform.PipelineJob(
        display_name="car_price_pipeline",
        template_path="retrain_pipeline.json",
        pipeline_root="gs://is3107-car-data/pipeline-artifacts"
    )

    job.run()