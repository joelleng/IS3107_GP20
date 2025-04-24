import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np # For log scale option

def create_visualizations(df):
    """
    Creates a comprehensive set of interactive visualizations for the car dataset.
    
    Args:
        df (pd.DataFrame): Input dataframe containing cleaned car data.
    """
    
    # --- Basic Distributions ---
    st.header("Key Feature Distributions")
    col1, col2 = st.columns(2)
    
    with col1:
        # 1. Price Distribution (Log Scale Option)
        st.subheader("Price Distribution")
        use_log_scale_price = st.checkbox("Use Log Scale for Price Axis", key='log_price_dist')
        x_axis_price = np.log10(df['price']) if use_log_scale_price and not df['price'].isnull().all() else df['price']
        x_label_price = "Price (Log10 Scale)" if use_log_scale_price else "Price (SGD)"
        fig_price_hist = px.histogram(df.dropna(subset=['price']), x=x_axis_price, nbins=50, title='Distribution of Car Prices')
        fig_price_hist.update_layout(xaxis_title=x_label_price)
        st.plotly_chart(fig_price_hist, use_container_width=True)
        st.caption("Shows the frequency of cars at different price points. Use the checkbox for a log scale if the distribution is heavily skewed.")

        # 3. Car Age Distribution
        st.subheader("Car Age Distribution")
        fig_age_hist = px.histogram(df.dropna(subset=['car_age']), x='car_age', nbins=max(1, int(df['car_age'].max() or 1)), title='Distribution of Car Age (Years)')
        st.plotly_chart(fig_age_hist, use_container_width=True)
        st.caption("Shows how many cars are listed for each age.")
        
    with col2:
        # 2. Mileage Distribution (Log Scale Option)
        st.subheader("Mileage Distribution")
        use_log_scale_mileage = st.checkbox("Use Log Scale for Mileage Axis", key='log_mileage_dist')
         # Add 1 for log(0) case if mileage can be 0
        df['mileage_log'] = np.log10(df['mileage'] + 1) 
        x_axis_mileage = df['mileage_log'] if use_log_scale_mileage else df['mileage']
        x_label_mileage = "Mileage (km, Log10 Scale)" if use_log_scale_mileage else "Mileage (km)"
        fig_mileage_hist = px.histogram(df.dropna(subset=['mileage']), x=x_axis_mileage, nbins=50, title='Distribution of Mileage')
        fig_mileage_hist.update_layout(xaxis_title=x_label_mileage)
        st.plotly_chart(fig_mileage_hist, use_container_width=True)
        st.caption("Shows the frequency of cars at different mileage readings.")

        # 4. COE Left Distribution
        st.subheader("COE Remaining Distribution")
        fig_coe_hist = px.histogram(df.dropna(subset=['coe_left']), x='coe_left', nbins=20, title='Distribution of Remaining COE (Months)')
        st.plotly_chart(fig_coe_hist, use_container_width=True)
        st.caption("Shows the distribution of the remaining Certificate of Entitlement duration in months.")

    st.markdown("---")

    # --- Relationships with Price ---
    st.header("Factors Influencing Price")
    col3, col4 = st.columns(2)

    with col3:
        # 5. Mileage vs. Price
        st.subheader("Mileage vs. Price")
        fig_mileage_price = px.scatter(df, x='mileage', y='price', title='Mileage vs. Price', 
                                       hover_data=['brand', 'car_age', 'vehicle_type'], opacity=0.6, 
                                       color='car_age', color_continuous_scale=px.colors.sequential.Viridis,
                                       labels={"price": "Price (SGD)", "mileage": "Mileage (km)", "car_age": "Car Age"})
        st.plotly_chart(fig_mileage_price, use_container_width=True)
        st.caption("Relationship between mileage and price, colored by car age. Generally, higher mileage means lower price.")

        # 7. COE Left vs. Price (Crucial for SG)
        st.subheader("COE Left vs. Price")
        fig_coe_price = px.scatter(df, x='coe_left', y='price', title='Remaining COE vs. Price',
                                   hover_data=['brand', 'car_age', 'vehicle_type'], opacity=0.6,
                                   color='car_age', color_continuous_scale=px.colors.sequential.Viridis,
                                   labels={"price": "Price (SGD)", "coe_left": "COE Left (Months)", "car_age": "Car Age"})
        st.plotly_chart(fig_coe_price, use_container_width=True)
        st.caption("Relationship between remaining COE (months) and price, colored by car age. Higher remaining COE generally commands a higher price.")
        
        # 9. Engine Capacity vs. Price
        st.subheader("Engine Capacity vs. Price")
        fig_engine_price = px.scatter(df, x='engine_capacity_cc', y='price', title='Engine Capacity vs. Price',
                                      hover_data=['brand', 'car_age', 'power'], opacity=0.6,
                                      color='power', color_continuous_scale=px.colors.sequential.Inferno,
                                      labels={"price": "Price (SGD)", "engine_capacity_cc": "Engine Capacity (cc)", "power": "Power (bhp)"})
        st.plotly_chart(fig_engine_price, use_container_width=True)
        st.caption("Relationship between engine capacity and price, colored by power output.")

    with col4:
        # 6. Car Age vs. Price
        st.subheader("Car Age vs. Price")
        fig_age_price = px.scatter(df, x='car_age', y='price', title='Car Age vs. Price',
                                   hover_data=['brand', 'mileage', 'vehicle_type'], opacity=0.6,
                                   color='mileage', color_continuous_scale=px.colors.sequential.Plasma,
                                   labels={"price": "Price (SGD)", "car_age": "Car Age (Years)", "mileage": "Mileage (km)"})
        st.plotly_chart(fig_age_price, use_container_width=True)
        st.caption("Relationship between car age and price, colored by mileage. Older cars generally have lower prices.")

        # 8. Depreciation per Year vs. Car Age
        st.subheader("Depreciation vs. Car Age")
        fig_depr_age = px.scatter(df, x='car_age', y='depreciation_per_year', title='Depreciation per Year vs. Car Age',
                                  hover_data=['brand', 'price', 'vehicle_type'], opacity=0.6,
                                  color='price', color_continuous_scale=px.colors.sequential.Cividis,
                                  labels={"depreciation_per_year": "Depreciation/Year (SGD)", "car_age": "Car Age (Years)", "price": "Price (SGD)"})
        st.plotly_chart(fig_depr_age, use_container_width=True)
        st.caption("Shows how the calculated depreciation per year changes with the age of the car, colored by current price.")

        # 10. OMV vs. Price (Original Market Value)
        st.subheader("OMV vs. Price")
        fig_omv_price = px.scatter(df, x='omv', y='price', title='Original Market Value (OMV) vs. Price',
                                   hover_data=['brand', 'car_age', 'arf'], opacity=0.6,
                                   color='arf', color_continuous_scale=px.colors.sequential.Magma,
                                   labels={"price": "Price (SGD)", "omv": "OMV (SGD)", "arf": "ARF (SGD)"})
        st.plotly_chart(fig_omv_price, use_container_width=True)
        st.caption("Relationship between the assessed OMV and the listing price, colored by Additional Registration Fee (ARF).")


    st.markdown("---")
    
    # --- Categorical Feature Analysis ---
    st.header("Categorical Feature Analysis")
    col5, col6 = st.columns(2)

    with col5:
        # 11. Price Distribution by Brand (Box Plot)
        st.subheader("Price Distribution by Brand")
        # Limit to top N brands for clarity or make selectable
        top_n_brands = 15
        top_brands = df['brand'].value_counts().nlargest(top_n_brands).index.tolist()
        df_filtered_brands = df[df['brand'].isin(top_brands)]
        fig_price_brand = px.box(df_filtered_brands, x='brand', y='price', title=f'Price Distribution for Top {top_n_brands} Brands',
                                 category_orders={"brand": top_brands}, # Order boxes by frequency or price median
                                 labels={"price": "Price (SGD)", "brand": "Car Brand"},
                                 color='brand') # Color boxes by brand
        fig_price_brand.update_layout(xaxis={'categoryorder':'median descending'}) # Order by median price
        st.plotly_chart(fig_price_brand, use_container_width=True)
        st.caption(f"Compares price distributions across the top {top_n_brands} most frequent car brands.")

        # 13. Price by Transmission Type
        st.subheader("Price by Transmission Type")
        fig_trans_price = px.box(df, x='transmission', y='price', title='Price Distribution by Transmission Type',
                                 labels={"price": "Price (SGD)", "transmission": "Transmission"},
                                 color='transmission')
        st.plotly_chart(fig_trans_price, use_container_width=True)
        st.caption("Compares price distributions for different transmission types.")
        
    with col6:
        # 12. Price Distribution by Vehicle Type (Violin Plot)
        st.subheader("Price Distribution by Vehicle Type")
        fig_price_vehicle = px.violin(df, x='vehicle_type', y='price', title='Price Distribution by Vehicle Type',
                                      labels={"price": "Price (SGD)", "vehicle_type": "Vehicle Type"},
                                      color='vehicle_type', box=True, points="outliers")
        st.plotly_chart(fig_price_vehicle, use_container_width=True)
        st.caption("Shows the price distribution shape for different vehicle types.")

        # 14. Price by Fuel Type
        st.subheader("Price by Fuel Type")
        fig_fuel_price = px.box(df, x='fuel_type', y='price', title='Price Distribution by Fuel Type',
                                labels={"price": "Price (SGD)", "fuel_type": "Fuel Type"},
                                color='fuel_type')
        st.plotly_chart(fig_fuel_price, use_container_width=True)
        st.caption("Compares price distributions for different fuel types.")
        
    # 15. Price by Number of Owners
    st.subheader("Price by Number of Owners")
    # Convert no_of_owners to string for categorical plotting if desired, or keep numeric
    df['no_of_owners_cat'] = df['no_of_owners'].astype(str) + ' Owners' 
    fig_owners_price = px.box(df.sort_values('no_of_owners'), x='no_of_owners_cat', y='price', title='Price Distribution by Number of Owners',
                             labels={"price": "Price (SGD)", "no_of_owners_cat": "Number of Owners"})
    st.plotly_chart(fig_owners_price, use_container_width=True)
    st.caption("Compares price distributions based on the number of previous owners.")

    st.markdown("---")

    # --- Correlation and Multivariate ---
    st.header("Correlation & Multivariate Analysis")
    
    # 16. Correlation Heatmap
    st.subheader("Feature Correlation Heatmap")
    # Select only potentially relevant numeric columns for heatmap
    numeric_cols_corr = df.select_dtypes(include=np.number).columns.tolist()
    # Exclude columns that might not be directly comparable or are IDs if any
    cols_to_exclude = ['manufactured_year', 'mileage_log', 'no_of_owners'] # Example exclusions
    numeric_cols_corr = [col for col in numeric_cols_corr if col not in cols_to_exclude and df[col].nunique() > 1] 
    
    if numeric_cols_corr: # Check if there are columns left
        corr_matrix = df[numeric_cols_corr].corr()
        fig_heatmap = px.imshow(corr_matrix, text_auto='.2f', aspect="auto", 
                               title='Correlation Matrix of Numerical Features',
                               color_continuous_scale='RdBu_r', # Red-Blue diverging scale
                               zmin=-1, zmax=1) # Ensure scale covers full range
        fig_heatmap.update_layout(height=700) # Adjust height if needed
        st.plotly_chart(fig_heatmap, use_container_width=True)
        st.caption("Shows correlation coefficients (Pearson) between numerical features. Red = positive, Blue = negative correlation. Values near +/- 1 indicate strong linear relationships.")
    else:
        st.warning("Not enough numeric columns with variance to generate a correlation heatmap.")

    with st.expander("Other Visualizations"):
        
        # Brand Distribution Bar Chart
        st.subheader("Top 10 Brands by Listing Count")
        brand_counts = df['brand'].value_counts().head(10)
        fig_bar_brand = px.bar(
            brand_counts,
            x=brand_counts.index,
            y=brand_counts.values,
            title="Top 10 Car Brands by Count",
            labels={'index': 'Brand', 'value': 'Number of Listings'},
            color=brand_counts.index,
            color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_bar_brand, use_container_width=True)

        # Color Distribution Treemap
        st.subheader("Color Distribution")
        color_counts = df['color'].value_counts().reset_index()
        color_counts.columns = ['color', 'count']
        # Define a color map or let Plotly assign colors
        color_map = {'Black':'#333', 'White':'#eee', 'Grey':'#aaa', 'Silver':'#ccc', 'Blue':'#6baed6', 'Red':'#e41a1c', 'Brown':'#a65628', 'Beige':'#ffffd4', 'Green':'#4daf4a', 'Others':'#bdbdbd'}
        # Map less frequent colors to 'Others' if needed for clarity
        
        fig_treemap = px.treemap(
            color_counts,
            path=[px.Constant("All Cars"), 'color'], # Add root node
            values='count',
            title="Proportion of Cars by Color",
            color='color',
            color_discrete_map=color_map # Apply custom colors
        )
        fig_treemap.update_traces(textinfo = "label+percent root")
        st.plotly_chart(fig_treemap, use_container_width=True)

