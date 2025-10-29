from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner' : "faraz",
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
}


@dag(
    dag_id="airflow_v9_fixed", 
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False 
)

def print_hello_etl():

    @task(multiple_outputs=True)
    def get_name(): # Renamed for clarity
        # Now RETURNS the value, which Airflow automatically pushes to XCom
        return {"name": "World" , "type" :"Temporary"} 

    @task()
    def get_age(): # Renamed for clarity
        # Now RETURNS the value, which Airflow automatically pushes to XCom
        return 23

    @task()
    def greeting(name, age , type):
        # Airflow automatically pulls 'name' and 'age' from the upstream tasks' XComs
        print(f"This is a {type} {name}, you are {age} years old")
      

    
    # These calls now establish the dependency AND pass the XCom futures
    name_dict = get_name()
    age_value = get_age()
    
    # The greeting task will wait for name_value and age_value to complete and use their returned values
    greeting(name=name_dict['name'] , type=name_dict['type']  , age=age_value)

greet_dag = print_hello_etl()