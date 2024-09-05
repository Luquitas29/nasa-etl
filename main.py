from datetime import datetime
from utils import retrieve_and_process_asteroids, connect_to_redshift, update_redshift_table
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Definir parámetros
start_date = '2024-07-08'
end_date = '2024-07-12'

# Acceder a las variables de entorno
load_dotenv()
api_key = os.getenv('API_KEY')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

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

with DAG(
    'asteroids_etl',
    default_args=default_args,
    description='Un DAG para obtener y procesar datos de asteroides y actualizarlos en Redshift',
    schedule_interval=None,  # Ajusta el intervalo según sea necesario
    catchup=False,  # Asegura que el DAG no ejecute tareas pasadas
) as dag:

    def retrieve_and_process_asteroids_task():
        return retrieve_and_process_asteroids(start_date, end_date, api_key)

    def update_redshift_table_task(df):
        engine = connect_to_redshift(user, password, host, port, database)
        if engine:
            update_redshift_table(engine, df)
    
    retrieve_task = PythonOperator(
        task_id='retrieve_asteroids',
        python_callable=retrieve_and_process_asteroids_task,
    )

    update_task = PythonOperator(
        task_id='update_redshift_table',
        python_callable=update_redshift_table_task,
        op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="retrieve_asteroids") }}'}
    )

    retrieve_task >> update_task
