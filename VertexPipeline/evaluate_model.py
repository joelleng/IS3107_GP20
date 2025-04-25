from kfp.dsl import component, Input, Output, Model, Metrics

@component(
    packages_to_install=["pandas","google-cloud-bigquery","scikit-learn","joblib","db-dtypes", "google-cloud-storage", "xgboost"],
    base_image="python:3.9"
)
def evaluate_model_direct(
    model_path: Input[Model],
    metrics: Output[Metrics]
):
    """Samples a small subset from the feature table, computes RMSE and R²."""
    import pandas as pd
    from joblib import load
    import math
    from sklearn.metrics import mean_squared_error, r2_score
    from google.cloud import bigquery, storage
    import json

    project = 'is3107-453814'
    dataset = 'car_dataset'
    FEATURE_TABLE = f"{project}.{dataset}.data-feature_engineered"

    client = bigquery.Client(project=project)
    # pull only a small sample for testing
    df = client.query(
        f"SELECT * FROM `{FEATURE_TABLE}` LIMIT 1000"
    ).to_dataframe()

    df.drop(columns=["id"], inplace=True)
    df = df.astype({col: int for col in df.select_dtypes(include='bool').columns})

    X_test = df.drop(columns=['price'])
    y_test = df['price']

    model = load(model_path.path + ".joblib")
    y_pred = model.predict(X_test)

    rmse = math.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    with open(metrics.path, 'w') as f:
        json.dump({"rmse": rmse, "r2_score": r2}, f)
    
    # upload the metrics JSON to the specific bucket
    BUCKET_NAME   = "is3107-bucket"
    BUCKET_FOLDER = "mlops"
    gcs_client = storage.Client()
    bucket     = gcs_client.bucket(BUCKET_NAME)
    blob_name  = f"{BUCKET_FOLDER}/latest_metrics.json"
    bucket.blob(blob_name).upload_from_filename(metrics.path)
