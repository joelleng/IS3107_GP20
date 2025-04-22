import streamlit as st
import pandas as pd
from visualization import create_visualizations
from prediction import predict_price, get_prediction_inputs
from revert_one_hot_encode import revert_one_hot_encoded_columns

# Set page config
st.set_page_config(page_title="Car Price Analysis & Prediction", layout="wide")

# Load or generate sample data
@st.cache_data
def load_data():
    df = pd.read_csv('processed_used_cars.csv')
    return revert_one_hot_encoded_columns(df)

df = load_data()

# App title
st.title("Car Price Analysis and Prediction")

# Create tabs
tab1, tab2 = st.tabs(["Data Visualization", "Price Prediction"])

# Visualization Tab
with tab1:
    st.header("Interactive Data Visualizations")
    create_visualizations(df)

# Prediction Tab
with tab2:
    st.header("Car Price Prediction")
    
    # Input form
    st.subheader("Enter Car Details")
    inputs = get_prediction_inputs()
    
    # Prediction button
    if st.button("Predict Price"):
        predicted_price = predict_price(inputs)
        st.success(f"Predicted Price: ${predicted_price:,.2f}")

if __name__ == "__main__":
    pass