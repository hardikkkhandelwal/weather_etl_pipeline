import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SNOWFLAKE_USER = 'HARDIK'
SNOWFLAKE_PASSWORD = 'aB3defGhIjKlMn'
SNOWFLAKE_ACCOUNT = 'PAXVTTH-FT66584'
SNOWFLAKE_DATABASE = 'WEATHER_DB'
SNOWFLAKE_SCHEMA = 'WEATHER_SCHEMA'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

def extract_weather_data():
    logging.info("Starting Extraction Phase...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": 19.0760, "longitude": 72.8777, "current_weather": True} # Mumbai

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info("Extraction Successfull")
        return response.json()['current_weather']

    except requests.exceptions.RequestException as e:
        logging.error(f"Extraction Failed: {e}")
        return None

def transform_weather_data(raw_data):
    logging.info("Starting Transformation Phase...")
    try:
        df = pd.DataFrame([raw_data])
        df['city'] = 'Mumbai'

        df.columns = df.columns.str.upper()
        df = df[['CITY','TIME','TEMPERATURE','WINDSPEED','WINDDIRECTION','WEATHERCODE']]
        logging.info("Transfomation Successful!")
        return df
    except Exception as e:
        logging.error(f"Transformation Failed: {e}")
        return None

def load_weather_data(df):
    logging.info("Starting Load Phase to Snowflake Cloud...")
    try:
        engine = create_engine(
            URL(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                warehouse=SNOWFLAKE_WAREHOUSE
            ))

        df.to_sql('DAILY_WEATHER', engine, if_exists='append', index=False)
        logging.info("Loading Successfult! Data is in the cloud")

    except Exception as e:
        logging.error(f"Failed to load data to snowflake: {e}")
    
if __name__ == '__main__':
    logging.info("Starting ETL Pipeline...")
    raw_data = extract_weather_data()
    if raw_data:
        weather_df = transform_weather_data(raw_data)
        if weather_df is not None:
            load_weather_data(weather_df)
        logging.info("== ETL PIPELINE COMPLETED ==")