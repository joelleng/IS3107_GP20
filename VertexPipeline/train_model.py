from kfp.dsl import component, Output, Model

@component(
    packages_to_install=["pandas","google-cloud-bigquery","scikit-learn","joblib","db-dtypes", "google-cloud-storage", "xgboost"],
    base_image="python:3.9"
)
def train_model_direct(
    model_path: Output[Model]
):
    """Loads feature-engineered table from BigQuery, trains with XGBoost, uploads model."""
    import pandas as pd
    from joblib import dump
    from xgboost import XGBRegressor
    from google.cloud import bigquery, storage


    project = 'is3107-453814'
    dataset = 'car_dataset'
    FEATURE_TABLE = f"{project}.{dataset}.data-feature_engineered"

    client = bigquery.Client(project=project)
    df = client.query(f"SELECT * FROM `{FEATURE_TABLE}`").to_dataframe()
    df.drop(columns=["id"], inplace=True)
    df = df.astype({col: int for col in df.select_dtypes(include='bool').columns})


    X = df.drop(columns=['price'])
    y = df['price']


    model = XGBRegressor(
        objective='reg:squarederror',
        subsample=0.6,
        reg_lambda=2,
        reg_alpha=1,
        n_estimators=300,
        max_depth=7,
        learning_rate=0.1,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X, y)


    # 1) dump locally
    dump(model, model_path.path + ".joblib")

    # 2) upload to GCS at specific bucket
    BUCKET_NAME   = "is3107-bucket"
    BUCKET_FOLDER = "mlops"
    gcs_client = storage.Client()
    bucket     = gcs_client.bucket(BUCKET_NAME)
    blob_name  = f"{BUCKET_FOLDER}/latest_model.joblib"
    bucket.blob(blob_name).upload_from_filename(model_path.path + ".joblib")