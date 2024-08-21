import requests
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

# Acceder a las variables de entorno
load_dotenv()
api_key = os.getenv('API_KEY')
user = os.getenv('user')
password = os.getenv('password')

def get_asteroids(start_date, end_date, api_key):
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data
    elif response.status_code == 400:
        print("Error 400: Bad Request. Verifica los parámetros de la solicitud.")
    elif response.status_code == 401:
        print("Error 401: Unauthorized. Verifica tu clave API.")
    elif response.status_code == 404:
        print("Error 404: Not Found. El recurso solicitado no existe.")
    elif response.status_code == 429:
        print("Error 429: Too Many Requests. Has excedido el límite de solicitudes. Intenta nuevamente más tarde.")
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
    df.reset_index(drop=True, inplace=True)  # Resetear el índice
    return df

# Definir parámetros
start_date = '2024-07-08'
end_date = '2024-07-12'

# Obtener los datos de los asteroides
asteroids_data = get_asteroids(start_date, end_date, api_key)

# Procesar los datos de los asteroides
asteroids_dict = process_asteroids_data(asteroids_data)

# Crear el DataFrame
df = create_asteroids_dataframe(asteroids_dict)

# Mostrar el DataFrame
# print(df)

def connect_to_redshift():
    # Definir  parámetros de la conexión
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port = '5439'
    database = 'data-engineer-database'
    
    # Crear cadena de conexión
    connection_string = f'redshift+psycopg2://{user}:{password}@{host}:{port}/{database}'
    
    # Crear motor de SQLAlchemy con opciones de configuración
    engine = create_engine(connection_string, connect_args={"options": "-csearch_path=public"})
    
    try:
        # Verificar la conexión utilizando la nueva API con 'text'
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Conexión exitosa:", result.fetchone())
            return engine
    except OperationalError as e:
        # Manejar errores de conexión
        print("Error al conectar con la base de datos:", e)
        return False

# Conectar a Redshift
engine = connect_to_redshift()

def update_redshift_table(df):
    # Especificar el esquema y la tabla
    schema = 'lucasmartinberardo_coderhouse'
    table_name = 'asteroids'
    temp_table_name = 'temp_asteroids'

    # Convertir las columnas
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
    df['close_approach_date'] = pd.to_datetime(df['close_approach_date'], errors='coerce').dt.date
    df['estimated_diameter_min'] = pd.to_numeric(df['estimated_diameter_min'], errors='coerce')
    df['estimated_diameter_max'] = pd.to_numeric(df['estimated_diameter_max'], errors='coerce')
    df['kilometers_per_hour'] = pd.to_numeric(df['kilometers_per_hour'], errors='coerce')
    df['kilometers'] = pd.to_numeric(df['kilometers'], errors='coerce')

    # Crear una conexión
    with engine.connect() as connection:
        # Crear tabla temporal con definición explícita de columnas
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

        # Insertar datos del DataFrame en la tabla temporal
        df.to_sql(temp_table_name, engine, schema=schema, if_exists='replace', index=False)
        print(f"Datos insertados en la tabla temporal {temp_table_name} en el esquema {schema}.")

        # Reemplazar datos en la tabla principal basados en la clave primaria (date, id)
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

# Llamar a la función para actualizar la tabla en Redshift
update_redshift_table(df)