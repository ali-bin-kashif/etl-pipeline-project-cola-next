from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import etl_pipeline 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 18),
    'email': ['alibinkashif007@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'cola_next_etl_dag',
    default_args=default_args,
    description='DAG for ETL Pipeline'
)

extract_data = PythonOperator(
    task_id='extract_data',
    dag=dag,
    python_callable=etl_pipeline.pipeline.extract_data
)

transform_data = PythonOperator(
    task_id='transform_data',
    dag=dag,
    python_callable=etl_pipeline.pipeline.transform_data
)

load_data = PythonOperator(
    task_id='load_data',
    dag=dag,
    python_callable=etl_pipeline.pipeline.load_data
)

extract_data >> transform_data >> load_data
