import os, re
import pandas as pd
from google.cloud import bigquery
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sentence_transformers import SentenceTransformer
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
import numpy as np
import seaborn as sns
import joblib

def feature_engineer_pca(df, n_components=7):

    numeric_cols = ['depreciation_per_year', 'coe_left', 'mileage', 'dereg_value', 
                    'omv', 'coe_value', 'arf', 'car_age', 'days_on_market',
                    'road_tax_per_year', 'engine_capacity_cc', 'power', 'curb_weight']
    pca_cols = [col for col in numeric_cols if col != 'no_of_owners'] # Number of owners is a discrete, categorical feature
    pca_df = df.copy()[pca_cols]

    scaled_data = StandardScaler().fit_transform(pca_df)

    pca_7 = PCA(n_components=n_components)
    pca_7_components = pca_7.fit_transform(scaled_data)

    pca_7_df = pd.DataFrame(pca_7_components, columns=[f'PC{i+1}' for i in range(7)], index=final_df.index)
    
    joblib.dump(pca_7, 'pca_7.joblib')

    non_pca_cols = df.drop(columns=pca_cols).copy()

    final_df = pd.concat([pca_7_df, non_pca_cols], axis=1)

    return final_df

def feature_engineer_dummy(df):
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
            df[col] = df[col].str.strip().str.lower()
    
    # Create dummies for categorical features
    df = pd.get_dummies(
        df, 
        columns=categorical_cols,
        prefix_sep='_',
        drop_first=True  # Reduces multicollinearity by dropping first category
    )
    return df

# def save_to_bigquery(df):


