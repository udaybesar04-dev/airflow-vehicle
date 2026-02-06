from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="first_dag",
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False
)
def first_dag():

    @task
    def first_task():
        print("this is first task")

    @task
    def second_task():
        print("this is second task")

    @task
    def third_task():
        print("this is third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


first_dag()
