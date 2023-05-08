from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jose-holgado',
    'retries': 5,
    'retry_interval': timedelta(minutes=5)
}


def get_sklearn():
    import sklearn
    print(f"scikit-learn with version: {sklearn.__version__}")


def get_plotly():
    import plotly
    print(f"plotly with version: {plotly.__version__}")


with DAG(
    default_args=default_args,
    dag_id='dag_with_python_dependencies_v02',
    start_date=datetime(2023, 5, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )

    task2 = PythonOperator(
        task_id='get_plotly',
        python_callable=get_plotly
    )

    task1 >> task2