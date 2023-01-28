from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
from sqlite3 import Error
import os

dag_path = os.getcwd()


def transform_data():
    # load data from csv
    booking = pd.read_csv(f"{dag_path}/raw_data/data_ingestion_dag/booking.csv")
    client = pd.read_csv(f"{dag_path}/raw_data/data_ingestion_dag/client.csv")
    hotel = pd.read_csv(f"{dag_path}/raw_data/data_ingestion_dag/hotel.csv")

    data: pd.DataFrame = pd.merge(booking, client, on="client_id")
    data.rename(columns={"name": "client_name", "type": "client_type"}, inplace=True)

    data = pd.merge(data, hotel, on="hotel_id")
    data.rename(columns={"name": "hotel_name"}, inplace=True)

    data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

    data = data.drop("address", 1)
    data.to_csv(f"{dag_path}/processed_data/data_ingestion_dag/processed_data.csv")


def load_data():
    conn = None
    try:
        conn = sqlite3.connect(
            f"{dag_path}/processed_data/data_ingestion_dag/export_db.db"
        )
        data: pd.DataFrame = pd.read_csv(	
            f"{dag_path}/processed_data/data_ingestion_dag/processed_data.csv"
        )
        print(data)
        data.to_sql("bookings", conn, if_exists="append", index=False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


default_args = {"owner": "CHAKER", "start_date": days_ago(5)}

ingestion_dag = DAG(
    "booking_dag",
    default_args=default_args,
    description="initial dag for training in data ingestion, basic booking dag",
    schedule_interval=timedelta(days=1),
    # setting to true will run the previous scheduled times of the dag that didn't run since start date is 5 days ago
    catchup=False,
)

task_1 = PythonOperator(
    task_id="data_transformation", python_callable=transform_data, dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id="data_loading", python_callable=load_data, dag=ingestion_dag
)

task_1 >> task_2
