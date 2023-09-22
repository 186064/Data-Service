
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

 

BQ_PROJECT = "dataservices-271014"
BQ_DATASET = "customer_task"
BQ_TABLE = "sales_data"
GCS_BUCKET = "taskbucket1"

 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

 

dag = DAG(
    dag_id='transfer_file_to_bq', #dag name
    default_args=default_args,
    description='Transfering file from gcs to bigquery',
    schedule_interval=None,
    catchup=False,
)

 

transfer_file_to_bq = GCSToBigQueryOperator(
    task_id='transfer_file_to_bq',
    bucket='taskbucket1',
    source_objects='my_folder/mydata.csv',
    destination_project_dataset_table='dataservices-271014.customer_task.sales_data',
    autodetect=True,
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
