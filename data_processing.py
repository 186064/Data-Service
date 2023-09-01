from google.cloud import bigquery
import pandas as pd
from io import StringIO
from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

 

class DataProcessor:
    def __init__(self, file_metadata):
        self.file_metadata = file_metadata

 

    def load_gcs_file_to_dataframe(self):
        client = storage.Client()
        metadata = self.file_metadata
        bucket_name, blob_name = metadata["file_path"].replace("gs://", "").split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        file_content = blob.download_as_text()
        column_names = metadata["column_names"]
        df = pd.read_csv(StringIO(file_content), sep=metadata["field_separator"], names=column_names)
        return df

 

    def load_dataframe_to_bigquery(self, df):
        client = bigquery.Client()
        metadata = self.file_metadata
        table_id = metadata["table_id"]
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

 

def create_file_metadata():
    file_metadata = {
        "file_key": "sample_key",
        "file_path": "gs://taskbucket1/my_folder/mydata.csv",
        "table_id": "dataservices-271014.customer_task.table_data",
        "field_separator": ",",
        "column_names": ["name", "sales"],
        "column_types": ["STRING", "INTEGER"],
        "is_nullable": [False, False]
    }
    return file_metadata

 

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
    schedule_interval=None,
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
