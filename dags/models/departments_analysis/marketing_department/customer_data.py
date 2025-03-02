import pandas as pd
from datetime import datetime

def calculate_customer_metrics(df):
    print("Creating customer metrics")

    # Convert 'Order Date' to datetime
    df['Order Date'] = pd.to_datetime(df['Order Date'], unit='ms')

    # Get the first and last date of order for each customer
    cust_min = df.groupby('Customer ID')['Order Date'].min().reset_index()
    cust_min.rename(columns={'Order Date': 'First Date'}, inplace=True)

    cust_max = df.groupby('Customer ID')['Order Date'].max().reset_index()
    cust_max.rename(columns={'Order Date': 'Last Date'}, inplace=True)

    # Merge first and last trading day data
    cust = cust_min.merge(cust_max, how='inner', on='Customer ID')

    # Calculate customer tenure (years since first purchase)
    cust['Customer Tenure'] = ((cust['Last Date'] - cust['First Date']).dt.days / 365).astype(int)

    # Calculate recency (days since last purchase)
    cust['Recency'] = (cust['Last Date'].max() - cust['Last Date']).dt.days

    # Calculate frequency (number of orders)
    cust_freq = df.groupby('Customer ID')['Order ID'].size().reset_index()
    cust_freq.rename(columns={'Order ID': 'Frequency'}, inplace=True)

    # Calculate monetary value (total revenue)
    cust_revenue = df.groupby('Customer ID')['SalesAmountWithShipping'].sum().reset_index()
    cust_revenue.rename(columns={'SalesAmountWithShipping': 'Monetary'}, inplace=True)

    # Join RFM metrics
    cust = cust.merge(cust_freq, how='inner', on='Customer ID').merge(cust_revenue, how='inner', on='Customer ID')

    # Other metrics
    cust_quantity = df.groupby('Customer ID')['Quantity'].sum().reset_index()
    cust_quantity.rename(columns={'Quantity': 'Total Quantity'}, inplace=True)

    cust_first_order_revenue = df.groupby('Customer ID').apply(lambda x: (x['Unit Price'] * x['Quantity']).iloc[0]).reset_index(name='First Order Revenue')
    cust_days_between_orders = df.groupby('Customer ID')['Order Date'].apply(lambda x: x.diff().mean().days if len(x) > 1 else 0).reset_index(name='Avg Days Between Orders')
    cust_first_order_quantity = df.groupby('Customer ID')['Quantity'].first().reset_index(name='First Order Quantity')

    # Join additional metrics
    cust = (cust.merge(cust_quantity, how='inner', on='Customer ID')
                .merge(cust_first_order_revenue, how='inner', on='Customer ID')
                .merge(cust_days_between_orders, how='inner', on='Customer ID')
                .merge(cust_first_order_quantity, how='inner', on='Customer ID'))

    # Derived metrics
    cust['Average Order Value'] = cust['Monetary'] / cust['Frequency']
    cust['Customer Lifetime Value'] = cust['Monetary'] * cust['Customer Tenure']
    cust['Repeat Purchase Customer'] = (cust['Frequency'] > 1).astype(int)
    cust['Relative Repeat Rate'] = cust['Frequency'] / cust['Frequency'].max()
    cust['Churn Likelihood'] = cust['Recency'] / 365
    cust['Average Basket Size'] = cust['Total Quantity'] / cust['Frequency']

    high_value_threshold = cust['Customer Lifetime Value'].quantile(0.75)  # Top 25% CLV
    low_value_threshold = cust['Customer Lifetime Value'].quantile(0.25)   # Bottom 25% CLV

    # Assign customer segments
    cust['Customer Segment'] = pd.cut(cust['Customer Lifetime Value'], 
                                    bins=[-float('inf'), low_value_threshold, high_value_threshold, float('inf')],
                                    labels=['Low Value', 'Medium Value', 'High Value'])

    return cust