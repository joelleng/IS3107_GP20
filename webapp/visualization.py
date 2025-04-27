import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

def create_visualizations(df):
    st.header("Key Feature Distributions")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Price Distribution")
        use_log_scale_price = st.checkbox("Use Log Scale for Price Axis", key='log_price_dist')
        x_axis_price = np.log10(df['price']) if use_log_scale_price and not df['price'].isnull().all() else df['price']
        x_label_price = "Price (Log10 Scale)" if use_log_scale_price else "Price (SGD)"
        fig_price_hist = px.histogram(df.dropna(subset=['price']), x=x_axis_price, nbins=50, title='Distribution of Car Prices')
        fig_price_hist.update_layout(xaxis_title=x_label_price)
        st.plotly_chart(fig_price_hist, use_container_width=True)
        st.caption("Shows the frequency of cars at different price points.")

        st.subheader("Car Age Distribution")
        fig_age_hist = px.histogram(df.dropna(subset=['car_age']), x='car_age', nbins=max(1, int(df['car_age'].max() or 1)), title='Distribution of Car Age (Years)')
        st.plotly_chart(fig_age_hist, use_container_width=True)
        st.caption("Shows how many cars are listed for each age.")

    with col2:
        st.subheader("Mileage Distribution")
        use_log_scale_mileage = st.checkbox("Use Log Scale for Mileage Axis", key='log_mileage_dist')
        df['mileage_log'] = np.log10(df['mileage'] + 1)
        mileage_filtered = df[df['mileage'] <= 300000]
        x_axis_mileage = mileage_filtered['mileage_log'] if use_log_scale_mileage else mileage_filtered['mileage']
        x_label_mileage = "Mileage (km, Log10 Scale)" if use_log_scale_mileage else "Mileage (km)"
        fig_mileage_hist = px.histogram(mileage_filtered.dropna(subset=['mileage']), x=x_axis_mileage, nbins=50, title='Distribution of Mileage')
        fig_mileage_hist.update_layout(xaxis_title=x_label_mileage)
        st.plotly_chart(fig_mileage_hist, use_container_width=True)
        st.caption("Shows the frequency of cars at different mileage readings.")

        st.subheader("De-registration Value Distribution")
        dereg_99 = df['dereg_value'].dropna().quantile(0.99)
        dereg_filtered = df[df['dereg_value'] <= dereg_99]
        fig_dereg = px.histogram(dereg_filtered, x='dereg_value', nbins=50, title='Deregistration Value')
        st.plotly_chart(fig_dereg, use_container_width=True)
        st.caption("Shows the Distribution of deregistration value of cars")

    st.markdown("---")
    # --- Relationships with Price ---
    st.header("Factors Influencing Price")

    # Multiselect for brands with "All" option
    all_brands = sorted(df['brand'].dropna().unique())
    selected_brands = st.multiselect("Select Brands for Analysis", options=["All"] + all_brands, default=["All"])

    # Determine the filtered DataFrame
    if "All" in selected_brands or not selected_brands:
        filtered_df = df.copy()
    else:
        filtered_df = df[df['brand'].isin(selected_brands)]

    col3, col4 = st.columns(2)

    with col3:
        # 5. Mileage vs. Price
        st.subheader("Mileage vs. Price")
        fig_mileage_price = px.scatter(filtered_df, x='mileage', y='price', title='Mileage vs. Price', 
                                    hover_data=['brand', 'car_age', 'vehicle_type'], opacity=0.6)
        st.plotly_chart(fig_mileage_price, use_container_width=True)
        st.caption("Relationship between mileage and price for selected brands.")

        # 9. Engine Capacity vs. Price with average trend line
        st.subheader("Engine Capacity vs. Price")
        fig_engine = go.Figure()

        fig_engine.add_trace(go.Scatter(
            x=filtered_df['engine_capacity_cc'],
            y=filtered_df['price'],
            mode='markers',
            name='Cars',
            marker=dict(color='orange', opacity=0.6),
            hovertext=filtered_df['power']
        ))

        avg_line = filtered_df.groupby('engine_capacity_cc')['price'].mean().reset_index()
        fig_engine.add_trace(go.Scatter(
            x=avg_line['engine_capacity_cc'],
            y=avg_line['price'],
            mode='lines',
            name='Average Price',
            line=dict(color='blue', width=2)
        ))

        fig_engine.update_layout(title='Engine Capacity vs. Price with Trend Line',
                                xaxis_title='Engine Capacity (cc)',
                                yaxis_title='Price (SGD)')
        st.plotly_chart(fig_engine, use_container_width=True)
        st.caption("Trend line shows average price per engine capacity.")

    with col4:
        # 6. Car Age vs. Price
        st.subheader("Car Age vs. Price")
        fig_age_price = go.Figure()

        fig_age_price.add_trace(go.Scatter(
                    x=filtered_df['car_age'],
                    y=filtered_df['price'],
                    mode='markers',
                    name='Cars',
                    marker=dict(color='orange', opacity=0.6),
                    ))

        avg_line = filtered_df.groupby('car_age')['price'].mean().reset_index()
        fig_age_price.add_trace(go.Scatter(
            x=avg_line['car_age'],
            y=avg_line['price'],
            mode='lines',
            name='Average Price',
            line=dict(color='blue', width=2)
        ))

        fig_age_price.update_layout(title='Car vs. Price with Trend Line',
                                xaxis_title='Car Age (yrs)',
                                yaxis_title='Price (SGD)')
        st.plotly_chart(fig_age_price, use_container_width=True)
        st.caption("Relationship between car age and price for selected brands.")

        # 8. Depreciation per Year vs. Car Age
        st.subheader("Depreciation vs. Car Age")
        car_Age = filtered_df[['depreciation_per_year','car_age']].groupby('car_age').mean().reset_index()
        fig_depr_age = px.line(car_Age, x='car_age', y='depreciation_per_year', title='Depreciation per Year vs. Car Age')
        st.plotly_chart(fig_depr_age, use_container_width=True)
        st.caption("Shows how the calculated depreciation per year changes with the age of the car.")

    st.markdown("---")
    
    # --- Categorical Feature Analysis ---
    st.header("Categorical Feature Analysis")
    col5, col6 = st.columns(2)

    # 11. Price Distribution by Brand (Box Plot)
    st.subheader("Price Distribution by Brand")
    max_n = df['brand'].nunique()
    top_n_brands = st.slider("Number of Top Brands to Show", min_value=3, max_value=max_n, value=15, step=1)
    top_brands = df['brand'].value_counts().nlargest(top_n_brands).index.tolist()
    df_filtered_brands = df[df['brand'].isin(top_brands)]
    fig_price_brand = px.box(df_filtered_brands, x='brand', y='price', title=f'Price Distribution for Top {top_n_brands} Brands',
                            category_orders={"brand": top_brands},
                            labels={"price": "Price (SGD)", "brand": "Car Brand"},
                            color='brand')
    fig_price_brand.update_layout(xaxis={'categoryorder': 'median descending'})
    st.plotly_chart(fig_price_brand, use_container_width=True)
    st.caption(f"Compares price distributions across the top {top_n_brands} most frequent car brands.")

        
    with col5:
        # 12. Price Distribution by Vehicle Type (Violin Plot)
        st.subheader("Price Distribution by Vehicle Type")
        fig_price_vehicle = px.violin(df, x='vehicle_type', y='price', title='Price Distribution by Vehicle Type',
                                      labels={"price": "Price (SGD)", "vehicle_type": "Vehicle Type"},
                                      color='vehicle_type', box=True, points="outliers")
        st.plotly_chart(fig_price_vehicle, use_container_width=True)
        st.caption("Shows the price distribution shape for different vehicle types.")

    with col6:
        # 14. Price by Fuel Type
        st.subheader("Price by Fuel Type")
        fig_fuel_price = px.box(df, x='fuel_type', y='price', title='Price Distribution by Fuel Type',
                                labels={"price": "Price (SGD)", "fuel_type": "Fuel Type"},
                                color='fuel_type')
        st.plotly_chart(fig_fuel_price, use_container_width=True)
        st.caption("Compares price distributions for different fuel types.")

    # 15. Price by Number of Owners (Improved Violin Plot with Mean + Sorted)
    st.subheader("Price by Number of Owners")

    # Prepare categorical owner labels
    df['no_of_owners_cat'] = df['no_of_owners'].astype(str) + ' Owners'

    # Optional: Filter out top 1% price outliers to improve axis readability
    price_99 = df['price'].quantile(0.95)
    owners_filtered = df[df['price'] <= price_99]

    # Ensure correct order
    owners_filtered['no_of_owners_cat'] = owners_filtered['no_of_owners'].astype(str) + " Owners"
    owners_sorted = sorted(owners_filtered['no_of_owners'].dropna().unique())
    owners_cat_order = [f"{int(o)} Owners" for o in owners_sorted]


    fig_violin_owners = px.violin(
        owners_filtered,
        x='no_of_owners_cat',
        y='price',
        box=True,
        points='outliers', 
        title='Price Distribution by Number of Owners (Violin Plot)',
        color='no_of_owners_cat',
        labels={"no_of_owners_cat": "Number of Owners", "price": "Price (SGD)"}
    )

    fig_violin_owners.update_traces(meanline_visible=True, scalemode='count')
    fig_violin_owners.update_layout(xaxis=dict(categoryorder='array', categoryarray=owners_cat_order),
                                    xaxis_title="Number of Owners",
                                    yaxis_title="Price (SGD)",
                                    showlegend=False)

    st.plotly_chart(fig_violin_owners, use_container_width=True)
    st.caption("Violin plot shows distribution of price against number of owners. Price is limited to bottom 95% to reveal clearer differences.")
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

        # Color Distribution Treemap (Top 6 + Others, Enlarged)
        st.subheader("Color Distribution")

        # Count top 5 colors
        top_colors = df['color'].value_counts().nlargest(6)
        top_color_names = top_colors.index.tolist()

        # Group the rest as "Others"
        df['color_grouped'] = df['color'].apply(lambda x: x if x in top_color_names else "Others")
        color_counts = df['color_grouped'].value_counts().reset_index()
        color_counts.columns = ['color', 'count']

        # Define color background mappings (case-insensitive fallback if needed)
        default_map = {
            'Black': '#000000',
            'White': '#FFFFFF',
            'Grey': '#A9A9A9',
            'Silver': '#C0C0C0',
            'Blue': '#1E90FF',
            'Red': '#FF0000',
            'Brown': '#8B4513',
            'Beige': '#F5F5DC',
            'Green': '#228B22',
            'Yellow': '#FFD700',
            'Orange': '#FFA500',
            'Others': '#D3D3D3'
        }

        # Ensure all labels have colors
        used_colors = color_counts['color'].unique()
        color_map = {c: default_map.get(c, '#D3D3D3') for c in used_colors}

        fig_treemap = px.treemap(
            color_counts,
            path=[px.Constant("All Cars"), 'color'],
            values='count',
            title="Top 6 Car Colors (Others Grouped)",
            color=default_map.get('color'),
            color_discrete_map=color_map
        )
        fig_treemap.update_traces(textinfo="label+percent root")
        fig_treemap.update_layout(height=600)  # Made larger

        st.plotly_chart(fig_treemap, use_container_width=True)

