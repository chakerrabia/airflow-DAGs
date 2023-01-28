from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import os
import json
from requests import get, Response, HTTPError
import logging
from datetime import timedelta


dag_path = os.getcwd()

MY_SPOTFY_TOKEN = os.getenv("MY_SPOTFY_TOKEN", "")
MY_SPOTIFY_ENDPOINT = os.getenv("MY_SPOTIFY_ENDPOINT", "")


def get_data():
    try:
        response: Response = get(MY_SPOTIFY_ENDPOINT,  headers={'Authorization': f'Bearer {MY_SPOTFY_TOKEN}'})
        if response.status_code > 200:
            logging.error(f'Request failed with status {response.status_code}: {response.json()}')
            raise HTTPError
        with open(f"{dag_path}/raw_data/spotify_dag/spotify_raw.json", "w") as outfile:
            json.dump(response.json(), outfile)
    except Exception as e:
        logging.error(f"An error occured while fetching the data: {e}")


def process_data():
    pass


default_args = {"owner": "CHAKER", "start_date": days_ago(5)}

spotify_dag = DAG(
    dag_id="Spotify_Dag",
    description="Simple dag that grabs and transforms data from spotify API",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

task_1 = PythonOperator(task_id="get_data", python_callable=get_data, dag=spotify_dag)
task_2 = PythonOperator(
    task_id="process_data", python_callable=process_data, dag=spotify_dag
)

task_1 >> task_2
