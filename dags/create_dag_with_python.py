from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')

    print(f"Hello World!! My name is {first_name} {last_name} and I'm {age} years old")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jose')
    ti.xcom_push(key='last_name', value='Holgado')

def get_age(ti):
    ti.xcom_push(key='age', value=33)


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v06',
    description='First DAG using Python operator',
    start_date=datetime(2023, 5, 3),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )
    
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1