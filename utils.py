import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def get_asteroids(start_date, end_date, api_key):
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def process_asteroids_data(asteroids_data):
    asteroids_dict = {}
    if asteroids_data:
        for date in asteroids_data["near_earth_objects"]:
            for asteroid in asteroids_data["near_earth_objects"][date]:
                asteroid_info = {
                    "date": date,
                    "name": asteroid['name'],
                    "id": asteroid['id'],
                    "close_approach_date": asteroid['close_approach_data'][0]['close_approach_date'],
                    "estimated_diameter_min": asteroid['estimated_diameter']['meters']['estimated_diameter_min'],
                    "estimated_diameter_max": asteroid['estimated_diameter']['meters']['estimated_diameter_max'],
                    "kilometers_per_hour": asteroid['close_approach_data'][0]['relative_velocity']['kilometers_per_hour'],
                    "kilometers": asteroid['close_approach_data'][0]['miss_distance']['kilometers'],
                    "is_potentially_hazardous_asteroid": asteroid['is_potentially_hazardous_asteroid']
                }
                asteroids_dict[asteroid['id']] = asteroid_info
    return asteroids_dict

def create_asteroids_dataframe(asteroids_dict):
    df = pd.DataFrame.from_dict(asteroids_dict, orient='index')
    df.reset_index(drop=True, inplace=True)
    return df

def retrieve_and_process_asteroids(start_date, end_date, api_key):
    asteroids_data = get_asteroids(start_date, end_date, api_key)
    asteroids_dict = process_asteroids_data(asteroids_data)
    df = create_asteroids_dataframe(asteroids_dict)
    return df

def connect_to_redshift(user, password, host, port, database):
    connection_string = f'redshift+psycopg2://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string, connect_args={"options": "-csearch_path=public"})
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Conexión exitosa:", result.fetchone())
            return engine
    except OperationalError as e:
        print("Error al conectar con la base de datos:", e)
        return False

def transform_data(df):
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
    df['close_approach_date'] = pd.to_datetime(df['close_approach_date'], errors='coerce').dt.date
    df['estimated_diameter_min'] = pd.to_numeric(df['estimated_diameter_min'], errors='coerce')
    df['estimated_diameter_max'] = pd.to_numeric(df['estimated_diameter_max'], errors='coerce')
    df['kilometers_per_hour'] = pd.to_numeric(df['kilometers_per_hour'], errors='coerce')
    df['kilometers'] = pd.to_numeric(df['kilometers'], errors='coerce')
    return df

def update_redshift_table(engine, df):
    schema = 'lucasmartinberardo_coderhouse'
    table_name = 'asteroids'
    temp_table_name = 'temp_asteroids'

    df = transform_data(df)

    with engine.connect() as connection:
        create_temp_table_query = text(f"""
            CREATE TEMP TABLE {temp_table_name} (
                date DATE,
                name VARCHAR(255),
                id INTEGER,
                close_approach_date DATE,
                estimated_diameter_min DOUBLE PRECISION,
                estimated_diameter_max DOUBLE PRECISION,
                kilometers_per_hour DOUBLE PRECISION,
                kilometers DOUBLE PRECISION,
                is_potentially_hazardous_asteroid BOOLEAN
            );
        """)
        connection.execute(create_temp_table_query)
        print(f"Tabla temporal {temp_table_name} creada en el esquema {schema}.")

        df.to_sql(temp_table_name, engine, schema=schema, if_exists='replace', index=False)
        print(f"Datos insertados en la tabla temporal {temp_table_name} en el esquema {schema}.")

        merge_query = text(f"""
            BEGIN;
            DELETE FROM {schema}.{table_name}
            USING {schema}.{temp_table_name}
            WHERE {schema}.{table_name}.date = {schema}.{temp_table_name}.date
              AND {schema}.{table_name}.id = {schema}.{temp_table_name}.id;
            
            INSERT INTO {schema}.{table_name}
            SELECT * FROM {schema}.{temp_table_name};
            COMMIT;
        """)
        connection.execute(merge_query)
        print(f"Datos actualizados en la tabla {table_name} en el esquema {schema}.")


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar(email_user, email_password, subject, body_text, to_email):
    try:
        # Configurar el servidor SMTP
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email_user, email_password)

        # Crear el mensaje
        message = MIMEMultipart()
        message['From'] = email_user
        message['To'] = to_email
        message['Subject'] = subject

        message.attach(MIMEText(body_text, 'plain'))

        # Enviar el correo
        server.send_message(message)
        server.quit()

        print('Correo enviado exitosamente.')
    except Exception as e:
        print(f'Error al enviar el correo: {e}')

def detect_hazardous_asteroids_and_notify(email_user, email_password, df, start_date, end_date):
    """
    Detecta si hay asteroides potencialmente peligrosos en el DataFrame,
    imprime cuántos hay y sus nombres, y envía un correo si se detectan.
    """
    hazardous_asteroids = df[df['is_potentially_hazardous_asteroid'] == True]
    
    if not hazardous_asteroids.empty:
        count = hazardous_asteroids.shape[0]  # Número de asteroides peligrosos
        names = hazardous_asteroids['name'].tolist()  # Lista de nombres de los asteroides peligrosos
        
        print(f"Se han detectado {count} asteroides potencialmente peligrosos.")
        print("Nombres de los asteroides peligrosos:")
        for name in names:
            print(f"- {name}")

        # Preparar y enviar el correo
        subject = "Alerta: Asteroides potencialmente peligrosos detectados"
        body_text = f"Se han detectado {count} asteroides potencialmente peligrosos entre las fechas {start_date} y {end_date}.\n\nDetalles:\n{hazardous_asteroids.to_string(index=False)}"
        to_email = 'etllucas04@gmail.com'
        enviar(email_user, email_password, subject, body_text, to_email)
    else:
        print("No se detectaron asteroides potencialmente peligrosos.")
