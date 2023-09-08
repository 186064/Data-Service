from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import subprocess

def execute_sql_script():
    gcs_blob_path = "gs://taskbucket1/my_folder/Customerscript.sql"
    gsutil_cmd = ["gsutil", "cat", gcs_blob_path]

    try:
        sql_script = subprocess.check_output(gsutil_cmd, universal_newlines=True, stderr=subprocess.STDOUT)
        client = bigquery.Client()
        query_job = client.query(sql_script)
        for row in query_job:
            print(row)
    except subprocess.CalledProcessError as e:
        print("Error executing gsutil command:", e.output)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "execute_sql_to_gcs_dag",
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
) as dag:

    execute_sql_task = PythonOperator(
        task_id="execute_sql_task",
        python_callable=execute_sql_script,
    )
