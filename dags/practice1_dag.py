from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

@dag(
    dag_id = "pratice_dag",
    start_date = datetime(2026,2,6),
    schedule = "@hourly",
    catchup = False,
    tags = ["practice", "uday"]
)

def practice_dag():
    @task
    def extract():
        file_path = "/opt/airflow/data/raw_data_sql_practice.csv"
        df = pd.read_csv(file_path)
        print(f"extracted {len(df)} rows")
        return df.to_dict(orient="records")
        
    @task
    def preprocess(data):
        df = pd.DataFrame(data)
        df = df.dropna()
        df = df[df["vehicleSpeed"] >= 0]
        print(f"after preprocessing {len(df)}")
        return df.to_dict(orient="records")
        
    @task
    def transform(data):
        df = pd.DataFrame(data)
        df["is_overspeed"] = df["vehicleSpeed"] > 15
        print("transformed data :")
        return df.to_dict(orient="records")

    @task
    def load(data):
        df = pd.DataFrame(data)
        output_path = "/opt/airflow/data/cleaned_data.csv"
        df.to_csv(output_path, index=False)

        print(f"Saved cleaned datat to {output_path}")
    first = extract()
    second = preprocess(first)
    third = transform(second)
    load(third)

    first >> second >> third

practice_dag()