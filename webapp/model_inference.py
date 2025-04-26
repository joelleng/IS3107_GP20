import os
import pickle
import joblib
from datetime import datetime
import pandas as pd
from google.cloud import storage, bigquery
from sentence_transformers import SentenceTransformer

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../Airflow/keys/sa.json'
GCP_PROJECT_ID = 'is3107-453814'
BQ_DATASET = 'car_dataset'
FEATURE_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.data-feature_engineered"
BUCKET_NAME = 'is3107-bucket'
BUCKET_FOLDER = 'mlops'
PCA_BLOB = f'{BUCKET_FOLDER}/latest_pca.pkl'
SCALER_BLOB = f'{BUCKET_FOLDER}/latest_scaler.pkl'
MODEL_BLOB = f'{BUCKET_FOLDER}/latest_model.joblib'

# Numeric and categorical columns
PCA_NUMERIC_COLS = [
    'depreciation_per_year', 'coe_left', 'mileage', 'dereg_value',
    'omv', 'coe_value', 'arf', 'car_age', 'days_on_market',
    'road_tax_per_year', 'engine_capacity_cc', 'power', 'curb_weight'
]
CATEGORICAL_COLS = ['brand', 'color', 'fuel_type', 'transmission', 'vehicle_type']

# -----------------------------------------------------------------------------
# FEATURE ENGINEERING
# -----------------------------------------------------------------------------
def initial_data_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.drop_duplicates(inplace=True)
    df = df[df.get('active', True).astype(bool)]
    for col in ['listing_url', 'active']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    for col in ['scraped_datetime', 'posted_datetime', 'updated_datetime', 'registration_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def feature_engineer_car_age(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    current_year = datetime.now().year
    if 'manufactured_year' in df.columns:
        df['car_age'] = current_year - df['manufactured_year']
        df.drop(columns=['manufactured_year'], inplace=True)
    return df


def feature_engineer_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if {'scraped_datetime','posted_datetime'}.issubset(df.columns):
        df['days_on_market'] = (df['scraped_datetime'] - df['posted_datetime']).dt.days
    for col in ['posted_datetime','scraped_datetime','updated_datetime','registration_date']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    return df


def feature_engineer_clean_model_name(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'car_model' in df.columns:
        df['car_model'] = (
            df['car_model'].astype(str)
               .str.lower()
               .str.replace(r'[^\w\s]', '', regex=True)
               .str.strip()
        )
    return df


def feature_engineer_car_model_embedding(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'car_model' in df.columns:
        model = SentenceTransformer('all-MiniLM-L6-v2')
        embeddings = model.encode(df['car_model'].tolist())
        emb_df = pd.DataFrame(embeddings, index=df.index).add_prefix('embedding_')
        df.drop(columns=['car_model'], inplace=True)
        df = pd.concat([df, emb_df], axis=1)
    return df


def feature_engineer_dummys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                       .str.lower()
                       .str.replace(r'[^\w\s]', '', regex=True)
                       .str.strip()
            )
    df = pd.get_dummies(df, columns=[c for c in CATEGORICAL_COLS if c in df.columns], drop_first=True)
    return df

# -----------------------------------------------------------------------------
# GCS ARTIFACTS LOADING
# -----------------------------------------------------------------------------
def download_from_gcs(bucket_name: str, blob_name: str, local_path: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)


def load_pca():
    local_pca = '/tmp/latest_pca.pkl'
    download_from_gcs(BUCKET_NAME, PCA_BLOB, local_pca)
    with open(local_pca, 'rb') as f:
        return pickle.load(f)


def load_scaler():
    local_scaler = '/tmp/latest_scaler.pkl'
    download_from_gcs(BUCKET_NAME, SCALER_BLOB, local_scaler)
    with open(local_scaler, 'rb') as f:
        return pickle.load(f)


def load_model():
    local_model = '/tmp/latest_model.joblib'
    download_from_gcs(BUCKET_NAME, MODEL_BLOB, local_model)
    return joblib.load(local_model)

# -----------------------------------------------------------------------------
# MAIN PREDICTION
# -----------------------------------------------------------------------------
def predict_price(input_dict: dict) -> float:
    # 1. DataFrame
    df = pd.DataFrame([input_dict])
    # 2. Features
    df = initial_data_cleaning(df)
    df = feature_engineer_car_age(df)
    df = feature_engineer_temporal_features(df)
    df = feature_engineer_clean_model_name(df)
    df = feature_engineer_car_model_embedding(df)
    # 3. PCA
    pca = load_pca()
    scaler = load_scaler()
    X_num = df[PCA_NUMERIC_COLS]
    X_scaled = scaler.transform(X_num)
    pca_feats = pca.transform(X_scaled)
    pca_df = pd.DataFrame(pca_feats, columns=[f'PC{i+1}' for i in range(pca_feats.shape[1])])
    # 4. Combine
    df_non_num = df.drop(columns=PCA_NUMERIC_COLS)
    df_full = pd.concat([pca_df, df_non_num.reset_index(drop=True)], axis=1)
    # 5. Dummy
    df_dummy = feature_engineer_dummys(df_full)
    # 6. Align
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table = client.get_table(FEATURE_TABLE)
    feature_cols = [f.name for f in table.schema if f.name not in ('id','price')]
    X = df_dummy.reindex(columns=feature_cols, fill_value=0).values
    # 7. Predict
    model = load_model()
    return float(model.predict(X)[0])

# -----------------------------------------------------------------------------
# DEFAULTS FOR UI
# -----------------------------------------------------------------------------
def build_full_input(ui: dict) -> dict:
    """
    Merge UI inputs with default/fake values for fields not collected in the Streamlit form.
    """
    now = datetime.now().isoformat()
    current_year = datetime.now().year
    defaults = {
        'used_car_id': 0,
        'listing_url': '',
        'car_model': 'Generic Model',
        'color': 'Unknown',
        'fuel_type': 'Petrol',
        'price': 0.0,
        'depreciation_per_year': 0.0,
        'registration_date': now,
        'scraped_datetime': now,
        'posted_datetime': now,
        'updated_datetime': now,
        'manufactured_year': current_year - ui.get('car_age', 0),
        'road_tax_per_year': 0.0,
        'transmission': 'Automatic',
        'dereg_value': 0.0,
        'omv': 0.0,
        'coe_value': 0.0,
        'arf': 0.0,
        'curb_weight': 0.0,
        'active': True
    }
    return {**defaults, **ui}