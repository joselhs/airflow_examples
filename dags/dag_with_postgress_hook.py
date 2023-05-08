import csv
import logging
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args={
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_s3(ds_nodash, next_ds_nodash):
    # Step 1: query data from postgres db and saving to txt
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders WHERE delivery_date >= %s and delivery_date <= %s", (ds_nodash, next_ds_nodash))

    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
    
        # step2: load data to S3
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_file(
            filename=f.name,
            bucket_name='airflow',
            key=f"orders/get_orders_{ds_nodash}.txt",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)
        

with DAG(
    dag_id='dag_with_postgres_hook_v04',
    default_args=default_args,
    start_date=datetime(2023, 4, 1),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
