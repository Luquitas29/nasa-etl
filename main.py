from datetime import datetime
from utils import retrieve_and_process_asteroids, connect_to_redshift, update_redshift_table, detect_hazardous_asteroids_and_notify
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Definir parámetros
start_date = '2024-07-08'
end_date = '2024-07-12'

# Acceder a las variables de entorno
load_dotenv()
api_key = os.getenv('API_KEY')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
email_user = os.getenv('EMAIL_USER')
email_password = os.getenv('EMAIL_PASSWORD')

# Parámetros conexión
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
database = 'data-engineer-database'


# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),  # Ajusta esta fecha según sea necesario
    'retries': 1,
}

def save_dataframe_to_csv(df, file_path):
    df.to_csv(file_path, index=False)

def retrieve_and_process_asteroids_task():
    df = retrieve_and_process_asteroids(start_date, end_date, api_key)
    file_path = '/tmp/asteroids_data.csv'
    save_dataframe_to_csv(df, file_path)
    return file_path

def update_redshift_table_task(file_path):
    df = pd.read_csv(file_path)
    engine = connect_to_redshift(user, password, host, port, database)
    if engine:
        update_redshift_table(engine, df)

def detect_hazardous_and_notify_task(file_path, email_user, email_password):
    df = pd.read_csv(file_path)
    detect_hazardous_asteroids_and_notify(email_user, email_password, df, start_date, end_date)


with DAG(
    'asteroids_etl',
    default_args=default_args,
    description='Un DAG para obtener y procesar datos de asteroides, actualizarlos en Redshift y notificar sobre asteroides peligrosos',
    schedule_interval=None,  # Ajusta el intervalo según sea necesario
    catchup=False,  # Asegura que el DAG no ejecute tareas pasadas
) as dag:

    retrieve_task = PythonOperator(
        task_id='retrieve_asteroids',
        python_callable=retrieve_and_process_asteroids_task,
    )

    update_task = PythonOperator(
        task_id='update_redshift_table',
        python_callable=update_redshift_table_task,
        op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="retrieve_asteroids") }}'}
    )

    detect_hazardous_task = PythonOperator(
        task_id='detect_hazardous_and_notify',
        python_callable=detect_hazardous_and_notify_task,
        op_kwargs={
            'file_path': '{{ task_instance.xcom_pull(task_ids="retrieve_asteroids") }}',
            'email_user': email_user,
            'email_password': email_password
        }
    )

    retrieve_task >> update_task >> detect_hazardous_task