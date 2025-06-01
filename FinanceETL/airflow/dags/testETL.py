from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os

# Define proper paths (works in both Windows and Linux)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(DAG_DIR, 'Finance_data.csv')
OUTPUT_PATH = os.path.join(DAG_DIR, 'transformed_finance_data.csv')

def extract_data(**kwargs):
    try:
        df = pd.read_csv(DATA_PATH)
        print(f"Successfully loaded data from {DATA_PATH}")
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def transform_data(**kwargs):
    ti = kwargs['ti']
    try:
        df = ti.xcom_pull(task_ids='extract_data')  # Fixed typo here
        df_transformed = df[df['Investment_Avenues'] == 'Yes']
        df_transformed.to_csv(OUTPUT_PATH, index=False)
        print(f"Data transformed and saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        raise

def load_data(**kwargs):
    try:
        df = pd.read_csv(OUTPUT_PATH)
        print("Loaded data preview:")
        print(df.head())
    except Exception as e:
        print(f"Error loading transformed data: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'finance_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for finance data',
    schedule='@daily',
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