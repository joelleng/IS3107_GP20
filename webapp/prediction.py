import streamlit as st

def get_prediction_inputs():
    """
    Collect input features for price prediction.
    
    Returns:
        dict: Dictionary of input features
    """
    inputs = {}
    
    col1, col2 = st.columns(2)
    
    with col1:
        inputs['mileage'] = st.number_input("Mileage (km)", min_value=0, value=50000, step=1000)
        inputs['car_age'] = st.number_input("Car Age (years)", min_value=0, value=5, step=1)
        inputs['engine_capacity_cc'] = st.number_input("Engine Capacity (cc)", min_value=0, value=2000, step=100)
        inputs['power'] = st.number_input("Power (kW)", min_value=0, value=100, step=10)
    
    with col2:
        inputs['coe_left'] = st.number_input("COE Left (days)", min_value=0, value=2000, step=100)
        inputs['no_of_owners'] = st.number_input("Number of Owners", min_value=1, value=1, step=1)
        inputs['brand'] = st.selectbox("Brand", [
            "BMW", "Toyota", "Mercedes Benz", "Honda", "Audi", "Porsche", "Other"
        ])
        inputs['vehicle_type'] = st.selectbox("Vehicle Type", [
            "Sports Car", "Luxury Sedan", "SUV", "Mid-Sized Sedan", "MPV", "Stationwagon", "Others"
        ])
    
    return inputs

def predict_price(inputs):
    """
    Dummy function to predict car price based on inputs.
    
    Args:
        inputs (dict): Dictionary of input features
    
    Returns:
        float: Predicted price
    """
    # Dummy prediction logic (to be replaced with actual model)
    base_price = 30000
    price = (
        base_price
        - inputs['mileage'] * 0.1
        - inputs['car_age'] * 2000
        + inputs['engine_capacity_cc'] * 10
        + inputs['power'] * 100
        + inputs['coe_left'] * 5
        - inputs['no_of_owners'] * 1000
    )
    
    if inputs['brand'] in ["BMW", "Mercedes Benz", "Audi", "Porsche"]:
        price *= 1.5
    if inputs['vehicle_type'] in ["Sports Car", "Luxury Sedan"]:
        price *= 1.3
    
    return max(price, 1000)  # Ensure price is positive