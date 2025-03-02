from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd  

# Import ETL functions
from models.data_formating_model import data_extraction
from models.data_cleaning import data_cleaning_and_standardization
from models.departments_analysis.marketing_department.customer_data import calculate_customer_metrics as customer_metrics_function
from models.departments_analysis.marketing_department.customer_budgeting import risk_allocation
from models.departments_analysis.product_department.boston_matrix import boston_matrix_allocation

# Define default arguments for the DAG
default_args = {
    'owner': 'Innocent Nhamo',
    'start_date': datetime(2025, 2, 24),
    'retries': 1,  
}

# Define the DAG
dag = DAG(
    'my_first_dag',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False, 
    description='A simple tutorial DAG',
)

################### TASK OPERATIONS  #################################

def extract_data(**kwargs):
    """Extract data from the source."""
    try:
        df = data_extraction()
        kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())  
        print("Data Extraction Completed")
    except Exception as e:
        print(f"Error during data extraction: {e}")

def clean_data(**kwargs):
    """Clean and standardize the extracted data."""
    try:
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids='data_extraction', key='extracted_data')
        
        if extracted_data:
            df = pd.read_json(extracted_data) 
            df_cleaned = data_cleaning_and_standardization(df)
            kwargs['ti'].xcom_push(key='cleaned_data', value=df_cleaned.to_json())

            # Save to an appropriate path
            output_path = "/mnt/c/Users/takue/Documents/Data Science/Super Store/new datasets/cleaned_data.csv"
            df_cleaned.to_csv(output_path, index=False)

            print("Data Cleaning Completed")
            print(f"Number of rows after cleaning: {df_cleaned.shape[0]}")
        else:
            print("No data found for cleaning")
    except Exception as e:
        print(f"Error during data cleaning: {e}")

def process_customer_metrics(**kwargs):
    """Process customer metrics from the cleaned data."""
    try:
        ti = kwargs['ti']
        cleaned_data = ti.xcom_pull(task_ids='data_cleaning_and_standardization', key='cleaned_data')

        if cleaned_data:
            df = pd.read_json(cleaned_data)
            customer_data = customer_metrics_function(df)
            kwargs['ti'].xcom_push(key='customer_data', value=customer_data.to_json())

            # Save to an appropriate path
            output_path = "/mnt/c/Users/takue/Documents/Data Science/Super Store/new datasets/customer_metrics.csv"
            customer_data.to_csv(output_path, index=False)

            print(f"Customer metrics exported to {output_path}")
        else:
            print("No cleaned data found for processing")
    except Exception as e:
        print(f"Error during customer metrics processing: {e}")

def allocating_risk_budget(**kwargs):
    """Allocate risk budget based on customer data."""
    try:
        ti = kwargs['ti']
        customer_data = ti.xcom_pull(task_ids='calculate_customer_metrics', key='customer_data')

        if customer_data:
            df = pd.read_json(customer_data)
            risk_budget_data = risk_allocation(df, 2013) # Remember to put in the year that you want to do the analysis for

            # Save to an appropriate path
            output_path = "/mnt/c/Users/takue/Documents/Data Science/Super Store/new datasets/risk_budget.csv"
            risk_budget_data.to_csv(output_path, index=False)

            print(f"Risk budget data has been exported to {output_path}")
        else:
            print("No customer data found for risk budget allocation")
    except Exception as e:
        print(f"Error during risk budget allocation: {e}")

def boston_matrix_product_allocation(**kwargs):
    """Allocate products to different categories."""
    try:
        ti = kwargs['ti']
        product_data = ti.xcom_pull(task_ids='data_cleaning_and_standardization', key='cleaned_data')

        if product_data:
            df = pd.read_json(product_data)
            risk_budget_data = boston_matrix_allocation(df) 

            # Save to an appropriate path
            output_path = "/mnt/c/Users/takue/Documents/Data Science/Super Store/new datasets/boston_matrix.csv"
            risk_budget_data.to_csv(output_path, index=False)

            print(f"Boston Matrix product data has been exported to {output_path}")
        else:
            print("No data found for boston matrix allocation")
    except Exception as e:
        print(f"Error during boston matrix allocation: {e}")

################### TASKS #################################
    
data_extraction_task = PythonOperator(
    task_id='data_extraction', 
    python_callable=extract_data, 
    dag=dag
)

data_cleaning_task = PythonOperator(
    task_id='data_cleaning_and_standardization', 
    python_callable=clean_data, 
    dag=dag
)

customer_metrics_task = PythonOperator(
    task_id='calculate_customer_metrics', 
    python_callable=process_customer_metrics, 
    dag=dag
)

risk_budget_task = PythonOperator(
    task_id='calculate_risk_budget', 
    python_callable=allocating_risk_budget, 
    dag=dag
)

boston_matrix_task = PythonOperator(
    task_id='product_allocation_boston_matrix', 
    python_callable=boston_matrix_product_allocation, 
    dag=dag
)

# Define task dependencies
data_extraction_task >> data_cleaning_task
data_cleaning_task >> [customer_metrics_task, risk_budget_task, boston_matrix_task]
