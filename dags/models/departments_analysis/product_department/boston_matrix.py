import pandas as pd

def boston_matrix_allocation(df):
    """
    Classify products into the Boston Consulting Group (BCG) Matrix categories
    based on their market share and compound annual growth rate (CAGR).

    Parameters:
        df (pd.DataFrame): Input DataFrame containing product sales data.

    Returns:
        pd.DataFrame: DataFrame with product classifications.
    """
    # Convert 'Order Date' to datetime and extract the year
    df['Order Date'] = pd.to_datetime(df['Order Date'], unit='ms')
    df['Year'] = df['Order Date'].dt.year

    # Select only the required features
    product_data = df[['Product Name', 'Year', 'SalesAmountWithShipping']]

    # Group data by Product Name and Year, then sum Sales
    grouped_products = (
        product_data.groupby(['Product Name', 'Year'])['SalesAmountWithShipping']
        .sum()
        .reset_index()
    )

    # Calculate CAGR for each product
    cagr_of_products = grouped_products.groupby('Product Name').apply(compute_cagr)
    cagr_of_products = cagr_of_products.reset_index().rename(columns={0: 'CAGR'})

    # Calculate total sales and market share
    total_sales = grouped_products['SalesAmountWithShipping'].sum()
    grouped_sales = (
        product_data.groupby('Product Name')['SalesAmountWithShipping']
        .sum()
        .reset_index()
    )
    grouped_sales['MarketShare'] = (grouped_sales['SalesAmountWithShipping'] / total_sales) * 100

    # Calculate relative market share
    grouped_sales['RelativeMarketShare'] = grouped_sales['MarketShare'] / grouped_sales['MarketShare'].max()

    # Merge CAGR and Market Share data into a single DataFrame
    boston_matrix_data = grouped_sales.merge(cagr_of_products, on='Product Name', how='inner')

    # Classify products into BCG Matrix categories
    boston_matrix_data['Category'] = boston_matrix_data.apply(
        lambda row: classify_product(row, boston_matrix_data), axis=1
    )

    return boston_matrix_data

def compute_cagr(group):
    """
    Calculate the Compound Annual Growth Rate (CAGR) for a product.

    Parameters:
        group (pd.DataFrame): Grouped data for a single product.

    Returns:
        float: CAGR value.
    """
    initial_year = group['Year'].min()
    final_year = group['Year'].max()

    starting_value = group.loc[group['Year'] == initial_year, 'SalesAmountWithShipping'].values[0]
    ending_value = group.loc[group['Year'] == final_year, 'SalesAmountWithShipping'].values[0]

    t = final_year - initial_year

    # Avoid division by zero or invalid calculations
    if t == 0 or starting_value == 0:
        return 0

    return (ending_value / starting_value) ** (1 / t) - 1

def classify_product(row, boston_matrix_data):
    """
    Classify a product into one of the BCG Matrix categories.

    Parameters:
        row (pd.Series): Row of the DataFrame containing product data.
        boston_matrix_data (pd.DataFrame): DataFrame containing all product data.

    Returns:
        str: BCG Matrix category ('Star', 'Cash Cow', 'Question Mark', or 'Dog').
    """
    # Calculate the midpoint of the CAGR axis
    cagr_min = boston_matrix_data['CAGR'].min()
    cagr_max = boston_matrix_data['CAGR'].max()
    cagr_mid = (cagr_min + cagr_max) / 2

    if row['RelativeMarketShare'] > 0.5 and row['CAGR'] > cagr_mid:
        return 'Star'
    elif row['RelativeMarketShare'] > 0.5 and row['CAGR'] <= cagr_mid:
        return 'Cash Cow'
    elif row['RelativeMarketShare'] <= 0.5 and row['CAGR'] > cagr_mid:
        return 'Question Mark'
    else:
        return 'Dog'