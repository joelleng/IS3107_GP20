from kfp.dsl import component, Output, Model

@component(
    packages_to_install=[
        "pandas", "google-cloud-bigquery", "scikit-learn", "joblib", "db-dtypes"
    ],
    base_image="python:3.9"
)
def train_model_direct(model_path: Output[Model]):
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor
    from joblib import dump
    from google.cloud import bigquery

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

    model = RandomForestRegressor()
    model.fit(X, y)

    dump(model, model_path.path + ".joblib")