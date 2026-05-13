import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from airflow.hooks.base import BaseHook


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date = datetime(2026, 5, 12),
    catchup = False,
    tags = ['weather','etl', 'snowflake'],
)

def weather_etl_pipeline():
    @task
    def extract_weather_data() -> list:
        logging.info("Starting Extraction Phase for Multiple Cities...")
        url = "https://api.open-meteo.com/v1/forecast"
        
        # A dictionary of cities and their coordinates
        cities = {
            "Mumbai": {"lat": 19.0760, "lon": 72.8777},
            "New York": {"lat": 40.7128, "lon": -74.0060},
            "London": {"lat": 51.5074, "lon": -0.1278},
            "Tokyo": {"lat": 35.6762, "lon": 139.6503},
            "Sydney": {"lat": -33.8688, "lon": 151.2093},
            "Paris": {"lat": 48.8566, "lon": 2.3522},
            "Berlin": {"lat": 52.5200, "lon": 13.4050},
            "Dubai": {"lat": 25.2048, "lon": 55.2708},
            "Singapore": {"lat": 1.3521, "lon": 103.8198},
            "Los Angeles": {"lat": 34.0522, "lon": -118.2437}
        } # We are starting with 10 top cities to test! You can easily expand this to 50 later.

        all_city_data = []

        for city_name, coords in cities.items():
            params = {"latitude": coords["lat"], "longitude": coords["lon"], "current_weather": True}
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()['current_weather']
                data['city'] = city_name # Inject the city name into the raw data
                all_city_data.append(data)
                logging.info(f"Successfully fetched weather for {city_name}")
            except Exception as e:
                # If one city fails, we log the error but CONTINUE to the next city!
                logging.error(f"Failed to fetch weather for {city_name}: {e}")
                continue 

        logging.info(f"Extraction complete. Fetched data for {len(all_city_data)} cities.")
        return all_city_data # Now returning a LIST of dictionaries



    @task
    def transform_weather_data(raw_data_list: list) -> list:
        logging.info("Starting Transformation Phase...")
        try:
            # We now pass the entire list of dictionaries directly into Pandas!
            df = pd.DataFrame(raw_data_list)

            # Standardize column names
            df.columns = df.columns.str.upper()
            
            # Keep only the columns we care about
            df = df[['CITY','TIME','TEMPERATURE','WINDSPEED','WINDDIRECTION','WEATHERCODE']]
            logging.info(f"Transformation Successful! Processed {len(df)} rows.")
            
            # Convert back to a list of dictionary records for Airflow XCom
            return df.to_dict(orient='records')
        except Exception as e:
            logging.error(f"Transformation Failed: {e}")
            raise

    @task
    def load_weather_data(transformed_records: list):
        logging.info("Starting Load Phase to Snowflake Cloud...")
        try:
            # Reconstruct our DataFrame from the Airflow XCom dictionary
            df = pd.DataFrame(transformed_records)

            # Fetch the secure connection from Airflow
            conn = BaseHook.get_connection('snowflake_creds')
            
            engine = create_engine(
                URL(
                    user=conn.login,
                    password=conn.password,
                    account=conn.host,
                    database=conn.extra_dejson.get('database'),
                    schema=conn.schema,
                    warehouse=conn.extra_dejson.get('warehouse')
                ))


            # Load into Snowflake
            df.to_sql('DAILY_WEATHER', engine, if_exists='append', index=False)
            logging.info("Loading Successful! Data is in the cloud")

        except Exception as e:
            logging.error(f"Failed to load data to snowflake: {e}")
            raise

    # --- THIS IS THE MAGIC OF TASKFLOW API ---
    # We call the functions and pass the return values into the next function.
    # Airflow automatically understands this means: Extract -> Transform -> Load
    raw_weather = extract_weather_data()
    transformed_weather = transform_weather_data(raw_weather)
    load_weather_data(transformed_weather)


weather_etl_pipeline()