from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2023, 5, 4),
    schedule_interval='@daily'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt DATE,
                dag_id CHARACTER VARYING,
                PRIMARY KEY (dt, dag_id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ ds }}', '{{dag.dag_id}}')
        """
    )

    task3 = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            DELETE FROM dag_runs WHERE dt = '{{ ds }}' AND dag_id =  '{{dag.dag_id}}'
        """
    )

    task1 >> task3 >> task2