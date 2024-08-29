from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pascal_triangle import print_pascal_triangle

def print_pascal():
    print_pascal_triangle(10)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='DAG for Pascal Triangle',
    schedule_interval='44 11 * * *',
)

run_pascal = PythonOperator(
    task_id='run_pascal_triangle',
    python_callable=print_pascal,
    dag=dag,
)

run_pascal