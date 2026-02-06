from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def detect_issue():
	print("Checking System logs....")
	if random.choice([True,False]):
		raise Exception("Incident Detected")
	print("System Healthy")
def alert_team():
	print("Incident Detected Sending Alert !")

with DAG (
	dag_id = "ir_practice_workflow",
	start_date = datetime(2026,2,5),
	schedule_interval = "@hourly",
	catchup = False
) as dag:

	detect = PythonOperator(
		task_id = "detect_issue",
		python_callable = detect_issue,
		retries = 2
)

	alert = PythonOperator(
		task_id = "alert_team",
		python_callable = alert_team
)

detect >> alert
