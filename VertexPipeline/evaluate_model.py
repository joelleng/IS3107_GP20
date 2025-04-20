from kfp.dsl import component, Output, Input, Model, Metrics

@component(
    packages_to_install=["pandas", "google-cloud-bigquery", "scikit-learn", "joblib", "db-dtypes"],
    base_image="python:3.9"
)
def evaluate_model_direct(model_path: Input[Model], metrics: Output[Metrics]):
    import pandas as pd
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.model_selection import train_test_split
    from joblib import load
    import math
    from google.cloud import bigquery
    import json

    client = bigquery.Client(project="is3107-453814", location="US")

    query = """
        SELECT mileage, manufactured_year, coe_left, road_tax_per_year,
               dereg_value, omv, arf, engine_capacity_cc, power,
               curb_weight, no_of_owners, price
        FROM `is3107-453814.car_dataset.used_car`
        WHERE price IS NOT NULL
        LIMIT 500
    """
    df = client.query(query).to_dataframe()

    X = df.drop(columns=["price"])
    y = df["price"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = load(model_path.path + ".joblib")
    y_pred = model.predict(X_test)

    rmse = math.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # Save metrics so they appear in UI
    metrics_dict = {
        "rmse": rmse,
        "r2_score": r2
    }

    with open(metrics.path, "w") as f:
        json.dump(metrics_dict, f)