from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator


default_args = {
    'owner' : "faraz",
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
}

with DAG(
    dag_id="new_dag_v1_fixed",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=True 
) as dag:
    # Everything inside this block MUST have the same level of indentation

    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Hello World'"
    )

    # If you intended this line for task dependency, it should be removed or part of a chain (e.g., task1 >> task2)
    # If it was simply meant to be the task definition, then the line is redundant.
    # I've left it out as it serves no function in a complete DAG.
    
    # If you had a second task, the dependency would look like this:
    # task2 = BashOperator(task_id="task2", bash_command="echo 'Goodbye'")
    # task1 >> task2 # This is how you set the dependency