from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'Faraz',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5),
}


with DAG (
    dag_id="this_is_first_dag_v4",
    default_args=default_args,
    description="this is my first dag",
    start_date=datetime(2025, 7 , 10 , 2),
    schedule='@daily'

)as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'hello world'"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo 'this is second task'"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo 'this is third task'"
    )
    [task1 , task2] >> task3
