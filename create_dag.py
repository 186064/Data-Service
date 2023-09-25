import pandas as pd

path = 'C:/Users/186064/Downloads/New_Workflow_Form.csv'
df = pd.read_csv(path, header=None, names=['DAG_ID', 'VALUE'])

dag_id = None
description = None
bucket_name = None
source_path = None
destination_path = None
scheduling = None
project_id = None
dataset_id = None
table_id = None

 
for index, row in df.iterrows():
    if row['DAG_ID'] == 'DAG_ID':
        dag_id = row['VALUE'].strip()
    elif row['DAG_ID'] == 'DESCRIPTION':
        description = row['VALUE'].strip()
    elif row['DAG_ID'] == 'SCHEDULING':
        scheduling = row['VALUE'].strip()
    elif row['DAG_ID'] == 'SOURCE_PATH':
        source_path = row['VALUE'].strip()
    elif row['DAG_ID'] == 'DESTINATION_PATH':
        destination_path = row['VALUE'].strip()
    elif row['DAG_ID'] == 'GCS_BUCKET':
        bucket_name = row['VALUE'].strip()
    elif row['DAG_ID'] == 'BQ_PROJECT':
        project_id = row['VALUE'].strip()
    elif row['DAG_ID'] == 'BQ_DATASET':
        dataset_id = row['VALUE'].strip()
    elif row['DAG_ID'] == 'BQ_TABLE':
        table_id = row['VALUE'].strip()

dag_code = f"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BQ_PROJECT = "{project_id}"
BQ_DATASET = "{dataset_id}"
BQ_TABLE = "{table_id}"
GCS_BUCKET = "{bucket_name}"

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule={scheduling},
    catchup=False,
)

transfer_file_to_bq = GCSToBigQueryOperator(
    task_id='{dag_id}',
    bucket='{bucket_name}',
    source_objects='{source_path}',
    destination_project_dataset_table='{destination_path}',
    autodetect=True,
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
"""

filename = f"{dag_id}_dag.py"
with open(filename, 'w') as file:
    file.write(dag_code)

print(f"Saved DAG code to {filename}")
