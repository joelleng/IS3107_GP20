import pandas as pd

def revert_one_hot_encoded_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()  # Avoid modifying the original DataFrame

    # Define the prefixes that identify one-hot encoded groups
    groups = ['brand_', 'color_', 'fuel_type_', 'transmission_', 'vehicle_type_']

    for prefix in groups:
        # Get all columns that belong to this one-hot group
        cols = [col for col in df.columns if col.startswith(prefix)]

        if not cols:
            continue  # Skip if no matching columns found

        # Create a new column with the original category name (remove trailing underscore)
        new_col_name = prefix.rstrip('_')
        df[new_col_name] = df[cols].idxmax(axis=1).str.replace(prefix, '', regex=False)

        # Drop the one-hot columns
        df.drop(columns=cols, inplace=True)

    return df
