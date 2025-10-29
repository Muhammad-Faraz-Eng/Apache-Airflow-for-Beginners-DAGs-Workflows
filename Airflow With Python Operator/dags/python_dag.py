from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable


default_args = {
    'owner' : "faraz",
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'start_date' : datetime(2021, 1, 1),
}

def details(ti):
    ti.xcom_push(key="hobby", value="Coding")
    ti.xcom_push(key="name", value="Faraz")
    ti.xcom_push(key="age", value=23)
    

def print_hello(ti):
    hobby = ti.xcom_pull(key="hobby", task_ids="details")
    name = ti.xcom_pull(key="name", task_ids="details")
    age = ti.xcom_pull(key="age", task_ids="details")

    print(f"Hello {name}, you are {age} years old and your hobby is {hobby}")  

age = Variable.get("Age")

with DAG(
    dag_id="python_v7",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    details_task = PythonOperator(
        task_id="details",
        python_callable=details,
    )

    print_hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
        provide_context=True,
    )
    bash_task= BashOperator(
        task_id="bash_task",
        bash_command="echo 'Hello World!'",
    )


    details_task >> print_hello_task >> bash_task
