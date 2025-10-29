from __future__ import annotations

import pendulum
import json
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable


# Retrieve all four variables globally
USER_NAME = Variable.get("Name", default_var="Faraz")
USER_EMAIL = Variable.get("Email", default_var="faraz@mail.com")
API_KEY = Variable.get("api-key", default_var="NO_API_KEY")
# Deserialize JSON variable explicitly
USER_DETAILS_JSON = Variable.get("Details", deserialize_json=True, default_var='{}')



def print_dic():
    print("User Details JSON:")
    print(USER_DETAILS_JSON)

def print_name_email():
    print("User Name and Email:")
    print(f"Name: {USER_NAME}, Email: {USER_EMAIL}")


def api_key():
    print("API Key:")
    print(API_KEY)

with DAG(
    dag_id="testing_variable",
    start_date=pendulum.datetime(2025, 10, 6, tz="UTC"),
    schedule=None,
    catchup=False,
)as dag:

    start_task = BashOperator(
        task_id="new_task",
        bash_command="echo '--- Starting Variable Processing Pipeline ---'",
    )

    task_name_and_email = PythonOperator(
        task_id="print_name_email",
        python_callable=print_name_email,
    )

    task_api_key = PythonOperator(
        task_id="api_key",
        python_callable=api_key,
    )

    task_print_dic = PythonOperator(
        task_id="print_dic",
        python_callable=print_dic,
    )

    # Set the dependencies
    start_task >> task_name_and_email >> task_api_key >> task_print_dic