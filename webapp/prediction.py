import streamlit as st
from datetime import datetime

def get_prediction_inputs():
    """
    Collect input features for price prediction.

    Returns:
        dict: Dictionary of input features
    """
    inputs = {}

    st.markdown("### Car Basic Information")
    col1, col2 = st.columns(2)
    with col1:
        inputs['car_model'] = st.text_input("Car Model", value="Civic Hybrid")
        inputs['brand'] = st.selectbox("Brand", ["Honda", "Toyota", "BMW", "Mercedes Benz", "Audi", "Porsche", "Other"])
        inputs['color'] = st.selectbox("Color", ["Black", "White", "Silver", "Red", "Blue", "Grey", "Other"])
        inputs['fuel_type'] = st.selectbox("Fuel Type", ["Petrol", "Diesel", "Hybrid", "Electric"])
        inputs['transmission'] = st.selectbox("Transmission", ["Automatic", "Manual"])
        inputs['vehicle_type'] = st.selectbox("Vehicle Type", ["Passenger", "SUV", "Luxury Sedan", "Mid-Sized Sedan", "MPV", "Stationwagon", "Others"])

    with col2:
        inputs['registration_date'] = st.date_input("Registration Date", value=datetime(2018, 5, 10)).isoformat()
        inputs['manufactured_year'] = st.number_input("Manufactured Year", min_value=1990, max_value=datetime.now().year, value=2018, step=1)
        inputs['coe_left'] = st.number_input("COE Left (months)", min_value=0, value=48, step=1)
        inputs['mileage'] = st.number_input("Mileage (km)", min_value=0, value=40000, step=1000)
        inputs['no_of_owners'] = st.number_input("Number of Owners", min_value=1, value=1, step=1)

    st.markdown("### Technical & Financial Info")
    col3, col4 = st.columns(2)
    with col3:
        inputs['engine_capacity_cc'] = st.number_input("Engine Capacity (cc)", min_value=0, value=1497, step=100)
        inputs['power'] = st.number_input("Power (kW)", min_value=0, value=106, step=1)
        inputs['curb_weight'] = st.number_input("Curb Weight (kg)", min_value=0, value=1380, step=10)

    with col4:
        inputs['depreciation_per_year'] = st.number_input("Depreciation Per Year (S$)", min_value=0, value=2000, step=100)
        inputs['road_tax_per_year'] = st.number_input("Road Tax Per Year (S$)", min_value=0, value=600, step=50)
        inputs['dereg_value'] = st.number_input("Deregistration Value (S$)", min_value=0, value=5000, step=100)
        inputs['omv'] = st.number_input("OMV (S$)", min_value=0, value=30000, step=500)
        inputs['coe_value'] = st.number_input("COE Value (S$)", min_value=0, value=20000, step=500)
        inputs['arf'] = st.number_input("ARF (S$)", min_value=0, value=10000, step=500)

    return inputs

from model_inference import predict_price, build_full_input

def run_prediction_ui():
    """
    Streamlit UI for prediction
    """
    st.header("Car Price Prediction")
    st.subheader("Enter Car Details")
    user_inputs = get_prediction_inputs()

    if st.button("Predict Price"):
        try:
            full_input = build_full_input(user_inputs)
            predicted_price = predict_price(full_input)
            st.success(f"Predicted Price: S$ {predicted_price:,.2f}")
        except Exception as e:
            st.error(f"Prediction failed: {e}")
