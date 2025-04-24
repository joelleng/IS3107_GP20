import streamlit as st
import pandas as pd
import numpy as np # Required for log transform if used in visualization
from visualization import create_visualizations # Assuming this is your updated viz file
# Assuming you have these utility files/functions
from prediction import predict_price, get_prediction_inputs 
from revert_one_hot_encode import revert_one_hot_encoded_columns # Keep if needed

# --- Configuration ---
st.set_page_config(
    page_title="Car Price Analysis & Prediction", 
    page_icon="🚗", 
    layout="wide"
)
st.markdown("""
    <style>
    body {
        background-color: white;
    }
    * {
        font-family: 'Courier New', Courier, monospace !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- Data Loading ---
# Use st.cache_data for caching data loading
@st.cache_data 
def load_data(path='processed_used_cars.csv'):
    """Loads and potentially preprocesses the car data."""
    try:
        df = pd.read_csv(path)
        df = revert_one_hot_encoded_columns(df)
        # --- Data Cleaning/Preprocessing (Example) ---
        # Ensure numeric types where expected
        numeric_cols = ['price', 'depreciation_per_year', 'coe_left', 'mileage', 
                        'manufactured_year', 'road_tax_per_year', 'dereg_value', 
                        'omv', 'coe_value', 'arf', 'engine_capacity_cc', 'power', 
                        'curb_weight', 'no_of_owners', 'car_age', 'days_on_market']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce') # Coerce errors to NaN

        # Ensure object types for categorical
        object_cols = ['brand', 'color', 'fuel_type', 'transmission', 'vehicle_type']
        for col in object_cols:
             if col in df.columns:
                df[col] = df[col].astype(str).fillna('Unknown') # Fill NaNs in object cols
        
        return df
    except FileNotFoundError:
        st.error(f"Error: Data file not found at '{path}'. Please ensure it's in the correct directory.")
        return None # Return None if file not found
    except Exception as e:
        st.error(f"An error occurred during data loading: {e}")
        return None

df = load_data()

# --- App Structure ---
st.title("🚗 Car Price Analysis and Prediction")

# Check if data loading was successful
if df is not None:
    # Create tabs
    tab1, tab2 = st.tabs(["📊 Data Visualization", "⚙️ Price Prediction"])

    # --- Visualization Tab ---
    with tab1:
        st.header("Interactive Data Visualizations")
        st.markdown("Explore various aspects of the used car dataset.")
        # Call the function from visualization.py
        create_visualizations(df.copy()) # Pass a copy to prevent mutation issues

    # --- Prediction Tab ---
    with tab2:
        st.header("Car Price Prediction")
        
        # Input form (using the function from prediction.py)
        st.subheader("Enter Car Details")
        # Pass unique values from df for dropdowns/selects in get_prediction_inputs
        inputs = get_prediction_inputs() 
        
        # Prediction button
        if st.button("Predict Price"):
            # Ensure predict_price handles the input dictionary correctly
            predicted_price = predict_price(inputs) 
            if predicted_price is not None:
                 # Format the price nicely
                st.success(f"Predicted Price: S$ {predicted_price:,.2f}")
            else:
                st.error("Could not generate prediction. Please check inputs or API status.")
else:
    # Display a message if data loading failed
    st.warning("Data could not be loaded. Please check the data file and refresh.")

# Optional: Add a footer or info in the sidebar
st.sidebar.title("About")
st.sidebar.info(
    "This app provides visualizations and price predictions for used cars "
    "based on historical data. Predictions are indicative."
)

# Note: Ensure 'prediction.py' and 'visualization.py' are in the same directory 
# or properly installed as modules. 
# Ensure 'processed_used_cars.csv' exists.
