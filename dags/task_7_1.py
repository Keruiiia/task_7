from datetime import datetime, timedelta
import pandas as pd
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.decorators import task_group
from airflow.providers.mongo.hooks.mongo import MongoHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 9),
    'retries': 0,
}




def replace_null_values():
    df = pd.read_csv("/opt/airflow/data/tiktok_google_play_reviews.csv")
    df.fillna('-', inplace=True)
    output_file_path = "/opt/airflow/data/tiktok_without_nulls.csv"
    df.to_csv(output_file_path, index=False)



def sort_by_created_date():
    df = pd.read_csv("/opt/airflow/data/tiktok_without_nulls.csv")
    df.rename(columns={'at': 'created_date'}, inplace=True)
    df = df.sort_values('created_date')
    output_file_path = "/opt/airflow/data/sorted_tiktok_without_nulls.csv"
    df.to_csv(output_file_path, index=False)



def clean_content_column():
    df = pd.read_csv("/opt/airflow/data/sorted_tiktok_without_nulls.csv")
    pattern = r"[^a-zA-Z\s\.\,\?\!']"
    df['content'] = df['content'].apply(lambda x: re.sub(pattern, '', x))
    output_file_path = "/opt/airflow/data/sorted_tiktok_without_nulls_and_emojis.csv"
    df.to_csv(output_file_path, index=False)



def upload_to_mongodb():
    mongo_conn = MongoHook(mongo_conn_id="mongo_default")
    client = mongo_conn.get_conn()
    db = client.task_7_database
    collection = db.tiktok_collection
    df = pd.read_csv("/opt/airflow/data/sorted_tiktok_without_nulls_and_emojis.csv")
    data_dict = df.to_dict('records')
    collection.insert_many(data_dict)


with DAG('tiktok_processing',
         default_args=default_args,
         schedule_interval="@daily") as dag:

    file_sensor = FileSensor(task_id='file_sensor',
                             fs_conn_id="fs_default",
                             filepath="tiktok_google_play_reviews.csv",
                             poke_interval=30)

    @task_group()
    def data_processing_tasks():
        replace_null_values_task = PythonOperator(task_id='replace_null_values_task',
                                                  python_callable=replace_null_values)

        sort_by_created_date_task = PythonOperator(task_id='sort_by_created_date_task',
                                                   python_callable=sort_by_created_date)

        clean_content_column_task = PythonOperator(task_id='clean_content_column_task',
                                                   python_callable=clean_content_column)


    upload_to_mongodb_task = PythonOperator(task_id='upload_to_mongodb_task',
                                                python_callable=upload_to_mongodb)


    file_sensor >> data_processing_tasks() >> upload_to_mongodb_task

