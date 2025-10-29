from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime
import requests

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))

# Define the DAG using modern parameters
with DAG(
    dag_id='welcome_dag',
    start_date=datetime(2025, 10, 6), 
    schedule='0 23 * * *', 
    catchup=False,
    tags=['tutorial', 'welcome'],
) as dag:
    
    print_welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome,
    )

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )



    # Set the dependencies
    print_welcome_task >> print_date_task