import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def create_visualizations(df):
    """
    Create a comprehensive set of interactive visualizations for the car dataset.
    
    Args:
        df (pd.DataFrame): Input dataframe containing car data
    """
    # Main layout with two columns
    col1, col2 = st.columns(2)
    
    # Scatter Plot: Price vs Mileage with Vehicle Type Multi-Select
    with col1:
        st.subheader("Price vs Mileage")
        vehicle_types = sorted(df['vehicle_type'].unique())
        selected_vehicle_types = st.multiselect(
            "Select Vehicle Types",
            vehicle_types,
            default=vehicle_types,
            key="vehicle_type_scatter"
        )
        filtered_df = df[df['vehicle_type'].isin(selected_vehicle_types)] if selected_vehicle_types else df
        scatter_fig = px.scatter(
            filtered_df,
            x="mileage",
            y="price",
            color="vehicle_type",
            size="engine_capacity_cc",
            hover_data=["brand", "car_age"],
            title="Price vs Mileage (Colored by Vehicle Type, Sized by Engine Capacity)",
            labels={"price": "Price ($)", "mileage": "Mileage (km)"}
        )
        st.plotly_chart(scatter_fig, use_container_width=True)
    
    # Density Plot: Price Distribution with Brand Multi-Select
    with col2:
        st.subheader("Price Density by Brand")
        brands = sorted(df['brand'].unique())
        selected_brands = st.multiselect(
            "Select Brands",
            brands,
            default=brands[:3],  # Default to top 3 brands
            key="brand_density"
        )
        filtered_df = df[df['brand'].isin(selected_brands)] if selected_brands else df
        density_fig = px.histogram(
            filtered_df,
            x="price",
            color="brand",
            histnorm="density",
            nbins=50,
            title="Price Density by Brand",
            labels={"price": "Price ($)"},
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        density_fig.update_traces(opacity=0.7)
        st.plotly_chart(density_fig, use_container_width=True)
    
    # Correlation Heatmap
    st.subheader("Correlation Heatmap")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    corr_matrix = df[numeric_cols].corr()
    
    heatmap_fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.columns,
        colorscale='Plasma',  # Vibrant colors for dark theme
        showscale=True
    ))
    heatmap_fig.update_layout(
        title="Correlation Matrix of Numeric Features",
        height=600
    )
    st.plotly_chart(heatmap_fig, use_container_width=True)
    
    # Brand Distribution Bar Chart
    with st.expander("Brand Distribution"):
        st.subheader("Brand Distribution")
        brand_counts = df['brand'].value_counts().head(10)
        bar_fig = px.bar(
            x=brand_counts.index,
            y=brand_counts.values,
            title="Top 10 Car Brands by Count",
            labels={'x': 'Brand', 'y': 'Count'},
            color=brand_counts.index,
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        st.plotly_chart(bar_fig, use_container_width=True)
    
    # Violin Plot: Price by Vehicle Type
    with st.expander("Price by Vehicle Type"):
        st.subheader("Price Distribution by Vehicle Type")
        violin_fig = px.violin(
            df,
            x="vehicle_type",
            y="price",
            title="Price Distribution by Vehicle Type",
            labels={"price": "Price ($)", "vehicle_type": "Vehicle Type"},
            color="vehicle_type",
            box=True,
            points="outliers"
        )
        st.plotly_chart(violin_fig, use_container_width=True)
    
    # Average Mileage vs Car Age with Vehicle Type Multi-Select
    with st.expander("Average Mileage vs Car Age"):
        st.subheader("Average Mileage vs Car Age")
        vehicle_types = sorted(df['vehicle_type'].unique())
        selected_vehicle_types = st.multiselect(
            "Select Vehicle Types",
            vehicle_types,
            default=vehicle_types,
            key="vehicle_type_mileage"
        )
        filtered_df = df[df['vehicle_type'].isin(selected_vehicle_types)] if selected_vehicle_types else df
        avg_mileage = filtered_df.groupby(['car_age', 'vehicle_type'])['mileage'].mean().reset_index()
        mileage_fig = px.line(
            avg_mileage,
            x="car_age",
            y="mileage",
            color="vehicle_type",
            title="Average Mileage by Car Age (Colored by Vehicle Type)",
            labels={"mileage": "Average Mileage (km)", "car_age": "Car Age (Years)"}
        )
        st.plotly_chart(mileage_fig, use_container_width=True)
    
    # Treemap: Color Distribution
    with st.expander("Color Distribution"):
        st.subheader("Color Distribution")
        color_counts = df['color'].value_counts().reset_index()
        color_counts.columns = ['color', 'count']
        treemap_fig = px.treemap(
            color_counts,
            path=['color'],
            values='count',
            title="Proportion of Cars by Color",
            color='color',
            color_discrete_map={
                'Black': '#000000', 'Blue': '#1E90FF', 'Grey': '#808080',
                'White': '#FFFFFF', 'Red': '#FF0000', 'Silver': '#C0C0C0',
                'Green': '#008000'
            }
        )
        st.plotly_chart(treemap_fig, use_container_width=True)
    
    # Parallel Coordinates Plot (Main Features)
    with st.expander("Parallel Coordinates"):
        st.subheader("Multivariate Analysis (Main Features)")
        main_features = ['price', 'mileage', 'car_age', 'engine_capacity_cc', 'power', 'coe_left']
        norm_df = df[main_features].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
        norm_df['vehicle_type'] = df['vehicle_type']
        
        par_fig = px.parallel_coordinates(
            norm_df,
            color=norm_df.index,
            labels={col: col.replace('_', ' ').title() for col in norm_df.columns},
            title="Parallel Coordinates of Main Features (Colored by Vehicle Type)"
        )
        st.plotly_chart(par_fig, use_container_width=True)