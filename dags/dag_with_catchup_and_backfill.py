from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='dag_with_catchup_backfill_v02',
    start_date=datetime(2023, 4, 25),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )