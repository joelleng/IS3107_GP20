from kfp.dsl import component, Output, Model

@component(
    packages_to_install=[
        "pandas", "google-cloud-bigquery", "scikit-learn", "joblib", "db-dtypes", "sentence-transformers", "pickle5"
    ],
    base_image="python:3.9"
)
def train_model_direct(model_path: Output[Model]):
    import os, re
    import pandas as pd
    import numpy as np
    from joblib import dump
    import pickle5 as pickle
    from sklearn.ensemble import RandomForestRegressor
    from google.cloud import bigquery, storage
    from sklearn.preprocessing import StandardScaler
    from sentence_transformers import SentenceTransformer
    from sklearn.decomposition import PCA



    GCP_PROJECT_ID = 'is3107-453814'
    BQ_DATASET_ID = 'car_dataset'
    BQ_TABLE_ID = 'used_car'
    BUCKET_NAME = 'is3107-bucket'

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_key = f"{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    query = f"SELECT * FROM {table_key} WHERE used_car_id IS NOT NULL AND listing_url IS NOT NULL AND car_model IS NOT NULL AND brand IS NOT NULL AND color IS NOT NULL AND fuel_type IS NOT NULL AND price IS NOT NULL AND depreciation_per_year IS NOT NULL AND registration_date IS NOT NULL AND coe_left IS NOT NULL AND mileage IS NOT NULL AND manufactured_year IS NOT NULL AND road_tax_per_year IS NOT NULL AND transmission IS NOT NULL AND dereg_value IS NOT NULL AND omv IS NOT NULL AND coe_value IS NOT NULL AND arf IS NOT NULL AND engine_capacity_cc IS NOT NULL AND power IS NOT NULL AND curb_weight IS NOT NULL AND no_of_owners IS NOT NULL AND vehicle_type IS NOT NULL AND scraped_datetime IS NOT NULL AND posted_datetime IS NOT NULL AND updated_datetime IS NOT NULL AND active IS NOT NULL"

    df = client.query(query).to_dataframe()

    def initial_data_cleaning(df):
        """Performs initial cleaning on used car DataFrame.
        
        Args:
            df (pd.DataFrame): Raw used car data
            
        Returns:
            pd.DataFrame: Cleaned DataFrame ready for further processing
        """
        df = df.copy()

        # Data Cleaning 1: Remove duplicates
        df.drop_duplicates(inplace=True)

        # Data Cleaning 2: Drop where active==False -> this is when in the future we have future training where we want to set certain dataset/entries to be False
        df = df[df['active'].astype(bool)]

        # Data Cleaning 2: listing_url, active column as it should not be a feature in ML except used_car_id as it is needed for reference
        df.drop(columns=['listing_url', 'active'], inplace=True)

        # Data Cleaning 3: Convert scraped_datetime, posted_datetime, updated_datetime to datetime
        datetime_cols = ['scraped_datetime', 'posted_datetime', 'updated_datetime']
        df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime)

        return df
    
    def feature_engineer_car_age(df):
        """Adds a new column 'car_age' to the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame containing car data
            
        Returns:
            pd.DataFrame: DataFrame with 'car_age' column added
        """
        current_year = pd.to_datetime('now').year
        df = df.copy()
        df['car_age'] = current_year - df['manufactured_year']

        df.drop(columns=['manufactured_year'], inplace=True)
        
        return df
    
    def feature_engineer_temporal_features(df):
        df = df.copy()
        
        # 1. Days on market
        df['days_on_market'] = (df['scraped_datetime'] - df['posted_datetime']).dt.days
        
        # Drop original datetime columns (optional)
        df.drop(columns=['posted_datetime', 'scraped_datetime', 'updated_datetime', 'registration_date'], inplace=True)
        
        return df

    def feature_engineer_clean_model_name(df):
        # Standardize to lowercase, remove punctuation/extra spaces
        df = df.copy()
        df['car_model'] = df['car_model'].apply(lambda x:  re.sub(r'[^\w\s]', '', str(x).lower().strip()))
        return df
    
    def feature_engineer_car_model_embedding(df):
        df = df.copy()
        # Load pretrained model
        model = SentenceTransformer('all-MiniLM-L6-v2')
        df['car_model_embedding'] = list(model.encode(df['car_model'].astype(str)))
        return df
    
    def save_to_gcs(bucket_name, object_name, model):
        """Saves model to GCS bucket"""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        
        with blob.open('wb') as f:
            pickle.dump(model, f)

    def feature_engineer_dummys(df):
        """Creates dummy variables for categorical features.
        
        Args:
            df (pd.DataFrame): DataFrame containing car data
            
        Returns:
            pd.DataFrame: DataFrame with dummy variables added
        """
        df = df.copy()
        categorical_cols = ['brand', 'color', 'fuel_type', 'transmission', 'vehicle_type']
        # categorical_cols = ['brand', 'car_model', 'color', 'fuel_type', 'transmission', 'vehicle_type']

        for col in categorical_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r'[^\w\s]', '', regex=True)  # Removes punctuation
                )
        
        # Create dummies for categorical features
        df = pd.get_dummies(
            df, 
            columns=categorical_cols,
            prefix_sep='_',
            drop_first=True  # Reduces multicollinearity by dropping first category
        )
        
        return df

    df_cleaned = initial_data_cleaning(df)
    df_with_car_age = feature_engineer_car_age(df_cleaned)
    df_temporal = feature_engineer_temporal_features(df_with_car_age)
    df_cleaned_model = feature_engineer_clean_model_name(df_temporal)
    df_embedded = feature_engineer_car_model_embedding(df_cleaned_model)
    final_df = df_embedded.copy()
    final_df.drop(columns=['car_model'], inplace=True)
    embedding_df = pd.DataFrame(final_df["car_model_embedding"].tolist()).add_prefix("embedding_")
    final_df.drop(columns=["car_model_embedding"], inplace=True)
    final_df = pd.concat([final_df, embedding_df], axis=1)
    final_df.rename(columns={"used_car_id": "id"}, inplace=True)

    ### Upload this dataset to the data-cleaned_table if id not in the table
    table_key = f"{BQ_DATASET_ID}.data-cleaned_table"

    final_df.to_gbq(
        destination_table=table_key,
        project_id=GCP_PROJECT_ID,
        if_exists='replace'  # This will overwrite the entire table
    )

    ### Continue with other feature engineering steps
    numeric_cols = ['depreciation_per_year', 'coe_left', 'mileage', 'dereg_value', 
                    'omv', 'coe_value', 'arf', 'car_age', 'days_on_market',
                    'road_tax_per_year', 'engine_capacity_cc', 'power', 'curb_weight']
    pca_cols = [col for col in numeric_cols if col != 'no_of_owners' and not col.startswith('embedding_')] # Number of owners is a discrete, categorical feature
    pca_df = final_df.copy()[pca_cols]
    
    scaled_data = StandardScaler().fit_transform(pca_df[pca_cols])

    pca_7 = PCA(n_components=7)
    pca_7_components = pca_7.fit_transform(scaled_data)

    ### Save PCA components to Bucket
    save_to_gcs(BUCKET_NAME, 'latest_pca.pkl', pca_7)

    pca_7_df = pd.DataFrame(pca_7_components, columns=[f'PC{i+1}' for i in range(7)], index=final_df.index)

    non_pca_cols = final_df.drop(columns=pca_cols).copy()  # These include 'no_of_owners' and any other non-numeric cols


    combined_df = pd.concat([pca_7_df, non_pca_cols], axis=1)
    combined_df.head()

    df_dummy = feature_engineer_dummys(combined_df)

    ### Save the final feature engineered dataset to BigQuery
    table_key = f"{BQ_DATASET_ID}.data-feature_engineered"
    df_dummy.to_gbq(
        destination_table=table_key,
        project_id=GCP_PROJECT_ID,
        if_exists='replace'  # This will overwrite the entire table
    )

    df_dummy.drop(columns=["id"], inplace=True)
    df_dummy = df_dummy.astype({col: int for col in df_dummy.select_dtypes(include='bool').columns})

    X = df_dummy.drop(columns=["price"])
    y = df_dummy["price"]

    model = RandomForestRegressor()
    model.fit(X, y)

    dump(model, model_path.path + ".joblib")