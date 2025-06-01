from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os

# Define proper paths (works in both Windows and Linux)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(DAG_DIR, 'Finance_data.csv')
TRANSFORMED_DIR = os.path.join(DAG_DIR, 'transformed_data_gender')

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

    if 'gender' not in df.columns:
        raise ValueError("Column 'gender' not found in the dataset.")

    unique_gender = df['gender'].unique()

    # Filter data and save to individual files
    for gender_value in unique_gender:
        filtered_df = df[df['gender'] == gender_value]
        output_file = os.path.join(TRANSFORMED_DIR, f'transformed_finance_data_{gender_value}.csv')
        filtered_df.to_csv(output_file, index=False)
        print(f"Saved filtered data for Gender={gender_value} to {output_file}")

    # Push the list of transformed file paths to XCom for downstream tasks
    transformed_files = [os.path.join(TRANSFORMED_DIR, f'transformed_finance_data_{gender}.csv') for gender in unique_gender]
    ti.xcom_push(key='transformed_files', value=transformed_files)

def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_files = ti.xcom_pull(task_ids='transform_data', key='transformed_files')

    for file_path in transformed_files:
        print(f"Loading transformed data from {file_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'finance_etl_pipeline_gender',
    default_args=default_args,
    description='A simple ETL pipeline for finance data based on Gender',
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