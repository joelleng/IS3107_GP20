import streamlit as st
import pandas as pd
from prediction import get_prediction_inputs
from model_inference import predict_price, build_full_input
from visualization import create_visualizations
from revert_one_hot_encode import revert_one_hot_encoded_columns
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../Airflow/keys/sa.json'

st.set_page_config(
    page_title="Car Price Analysis & Prediction",
    page_icon="🚗",
    layout="wide"
)

st.markdown("""
<style>
body { background-color: white; }
* { font-family: 'Courier New', Courier, monospace !important; }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    try:

        GCP_PROJECT_ID = 'is3107-453814'
        BQ_DATASET_ID = 'car_dataset'
        BQ_TABLE_ID = 'data-cleaned_table'

        client = bigquery.Client(project=GCP_PROJECT_ID)
        table_key = f"{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        query = f"SELECT * FROM `{table_key}`"

        df = client.query(query).to_dataframe()

        # Revert one-hot encoding and do type conversions
        df = revert_one_hot_encoded_columns(df)
        numeric_cols = ['price','depreciation_per_year','coe_left','mileage','manufactured_year',
                        'road_tax_per_year','dereg_value','omv','coe_value','arf',
                        'engine_capacity_cc','power','curb_weight','no_of_owners','car_age','days_on_market']
        for col in numeric_cols:
            if col in df: df[col] = pd.to_numeric(df[col], errors='coerce')
        object_cols = ['brand','color','fuel_type','transmission','vehicle_type']
        for col in object_cols:
            if col in df: df[col] = df[col].astype(str).fillna('Unknown')
        return df

    except Exception as e:
        st.error(f"Error loading from BigQuery: {e}")
        return None

df = load_data()

st.title("🚗 Car Price Analysis and Prediction")

if df is not None:
    tab1, tab2 = st.tabs(["📊 Data Visualization", "⚙️ Price Prediction"])
    with tab1:
        st.header("Interactive Data Visualizations")
        create_visualizations(df.copy())
    with tab2:
        st.header("Car Price Prediction")
        ui_inputs = get_prediction_inputs()
        if st.button("Predict Price"):
            full_input = build_full_input(ui_inputs)
            price = predict_price(full_input)
            st.success(f"Predicted Price: S$ {price:,.2f}")
else:
    st.warning("Data could not be loaded. Please check the CSV file.")