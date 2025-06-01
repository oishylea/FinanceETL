from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os

# Define proper paths (works in both Windows and Linux)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(DAG_DIR, 'Finance_data.csv')
TRANSFORMED_DIR = os.path.join(DAG_DIR, 'transformed_data')

# Ensure the transformed directory exists
os.makedirs(TRANSFORMED_DIR, exist_ok=True)

def extract_data(**kwargs):
    try:
        df = pd.read_csv(DATA_PATH)
        print(f"Successfully loaded data from {DATA_PATH}")
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def transform_data(**kwargs):
    # Pull the raw data from XCom
    ti = kwargs['ti']
    raw_data_json = ti.xcom_pull(task_ids='extract_data', key='raw_data')
    df = pd.read_json(raw_data_json)

    # Ensure the 'PPF' column exists
    if 'PPF' not in df.columns:
        raise ValueError("Column 'PPF' not found in the dataset.")

    # Get unique values in the 'PPF' column
    unique_ppfs = df['PPF'].unique()

    # Filter data and save to individual files
    for ppf_value in unique_ppfs:
        filtered_df = df[df['PPF'] == ppf_value]
        output_file = os.path.join(TRANSFORMED_DIR, f'transformed_finance_data_{ppf_value}.csv')
        filtered_df.to_csv(output_file, index=False)
        print(f"Saved filtered data for PPF={ppf_value} to {output_file}")

    # Push the list of transformed file paths to XCom for downstream tasks
    transformed_files = [os.path.join(TRANSFORMED_DIR, f'transformed_finance_data_{ppf}.csv') for ppf in unique_ppfs]
    ti.xcom_push(key='transformed_files', value=transformed_files)

def load_data(**kwargs):
    # Pull the list of transformed files from XCom
    ti = kwargs['ti']
    transformed_files = ti.xcom_pull(task_ids='transform_data', key='transformed_files')

    # Perform any additional actions with the transformed files (e.g., move to cloud storage, etc.)
    for file_path in transformed_files:
        print(f"Loading transformed data from {file_path}")
        # Example: You could upload these files to S3 or another storage system here

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'finance_etl_pipeline_ppf',
    default_args=default_args,
    description='A simple ETL pipeline for finance data based on PPF',
    schedule='@yearly',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task