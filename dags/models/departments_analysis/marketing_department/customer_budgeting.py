import pandas as pd 

def risk_allocation(cust, latest_year):
    print("Creating risk allocation budget")
    print("""
        Description : Differentiate Spending Based on Customer Value
          Losing a high-value customer is much more costly than losing a low value customer. 
          Therefore, when allocating your retention budget think about different value-based segments and how likely they are to buy from you again. 
          For this you will need to be able to calculate the likelihood to buy for each segment. 
          The customers with a lower likelihood to buy are at greater risk of leaving you—and never make another purchase again.
    """)
    risk_data = cust.copy()

    # Ensure 'First Date' and 'Last Date' are datetime
    risk_data['First Date'] = pd.to_datetime(risk_data['First Date'], unit='ms')
    risk_data['Last Date'] = pd.to_datetime(risk_data['Last Date'], unit='ms')

    # Identify all past customers (who made their first purchase before the latest year)
    past_customers = risk_data[risk_data['First Date'].dt.year < latest_year].copy()

    # New customers (first purchase was in the latest year)
    new_customers = risk_data[risk_data['First Date'].dt.year == latest_year].copy()
    new_customers['Risk Level'] = 'New'

    # Retained customers (first purchase was before latest year & purchased in latest year)
    retained_customers = risk_data[
        (risk_data['First Date'].dt.year < latest_year) & 
        (risk_data['Last Date'].dt.year == latest_year)
    ].copy()

    # Apply Risk Level categorization for retained and past customers
    for df in [retained_customers, past_customers]:
        df['Risk Level'] = pd.cut(
            df['Average Order Value'], 
            bins=[-1, 110, 900, float('inf')], 
            labels=['Low', 'Med', 'High']
        )

    # Combine retained and new customers
    final_data = pd.concat([new_customers, retained_customers])

    # Aggregate customer counts per risk level
    customer_count = final_data.groupby('Risk Level')['Customer ID'].nunique()

    # Retention = (Retained customers in latest year) / (Total past customers in the same risk level)
    retention = (
        retained_customers.groupby('Risk Level')['Customer ID'].nunique() /
        past_customers.groupby('Risk Level')['Customer ID'].nunique()
    ).reindex(customer_count.index).fillna(0) * 100  # Handle missing risk levels

    # Compute $ at Risk = (Number of Customers * Retention * AOV)
    aov = final_data.groupby('Risk Level')['Average Order Value'].mean()
    at_risk_value = (customer_count * (retention / 100) * aov).fillna(0)

    # Compute Risk Allocation (% of total risk)
    risk_allocation_perc = (at_risk_value / at_risk_value.sum()).fillna(0) * 100

    # Create final summary DataFrame
    summary_df = pd.DataFrame({
        'Number of Customers': customer_count,
        f'{latest_year} AOV': aov,
        'Retention (%)': retention,
        '$ at Risk': at_risk_value,
        'Risk Allocation (%)': risk_allocation_perc
    }).fillna(0)  

    # Format the DataFrame
    summary_df[f'{latest_year} AOV'] = summary_df[f'{latest_year} AOV'].apply(lambda x: f"${x:,.2f}")
    summary_df['Retention (%)'] = summary_df['Retention (%)'].apply(lambda x: f"{x:.2f}%")
    summary_df['$ at Risk'] = summary_df['$ at Risk'].apply(lambda x: f"${x:,.2f}")
    summary_df['Risk Allocation (%)'] = summary_df['Risk Allocation (%)'].apply(lambda x: f"{x:.2f}%")

    return summary_df