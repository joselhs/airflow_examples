from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='dag_with_cron_expression_v04',
    start_date=datetime(2023, 4, 15),
    schedule_interval='0 0,12 * * *'
) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo Dag with Cron Expression'
    )

    task1
