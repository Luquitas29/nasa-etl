import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Acceder a API key
load_dotenv()
api_key = os.getenv('API_KEY')


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
start_date = '2024-07-01'
end_date = '2024-07-08'

# Obtener los datos de los asteroides
asteroids_data = get_asteroids(start_date, end_date, api_key)

# Procesar los datos de los asteroides
asteroids_dict = process_asteroids_data(asteroids_data)

# Crear el DataFrame
df = create_asteroids_dataframe(asteroids_dict)

# Mostrar el DataFrame
print(df)
