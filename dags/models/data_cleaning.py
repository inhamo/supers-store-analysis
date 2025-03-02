import pandas as pd
import numpy as np 

def data_cleaning_and_standardization(df):
    print("Executing data cleaning and standardization process...")

    # Trim column names to remove extra spaces
    df.columns = df.columns.str.strip() 

    # Convert Order Date and Ship Date to datetime format
    def parse_date(date_str):
        try:
            return pd.to_datetime(date_str, format='%m/%d/%Y')  
        except ValueError:
            return pd.to_datetime(date_str, format='%d/%m/%Y')  

    df['Order Date'] = df['Order Date'].apply(parse_date)
    df['Ship Date'] = df['Ship Date'].apply(parse_date)

    # Change data types for categorical columns
    categorical_columns = ['Order Priority', 'Customer Segment', 'Product Category', 
                           'Product Sub-Category', 'Ship Mode']
    
    for col in categorical_columns:
        df[col] = df[col].astype('category')

    # Handle duplicates
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate rows. Removing...")
        df.drop_duplicates(inplace=True)

    # Correcting Data Types for Numeric Columns
    numeric_columns = ['Unit Price', 'Shipping Cost', 'Quantity']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['SalesAmountWithoutShipping'] = df['Unit Price'] * df['Quantity']
    df['SalesAmountWithShipping'] = df['SalesAmountWithoutShipping'] + df['Shipping Cost']

    print("Data cleaning and standardization completed.")
    return df
