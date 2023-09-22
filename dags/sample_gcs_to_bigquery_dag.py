from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_processing import DataProcessor, create_file_metadata

 

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

 

dag = DAG(
    "sample_gcs_to_bigquery_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
)

 

file_metadata = create_file_metadata()
data_processor = DataProcessor(file_metadata)

 

load_gcs_file_task = PythonOperator(
    task_id="load_gcs_file_task",
    python_callable=data_processor.load_gcs_file_to_dataframe,
    dag=dag,
)

 

load_to_bigquery_task = PythonOperator(
    task_id="load_to_bigquery_task",
    python_callable=data_processor.load_dataframe_to_bigquery,
    op_args=[load_gcs_file_task.output],
    dag=dag,
)

 

load_gcs_file_task >> load_to_bigquery_task
