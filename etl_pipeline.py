import requests
import pandas as pd
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration using the new Snowflake Account Details
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
        logging.info("Extraction Successful")
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
        logging.info("Transformation Successful!")
        return df
    except Exception as e:
        logging.error(f"Transformation Failed: {e}")
        return None

def load_weather_data(df):
    logging.info("Starting Load Phase to Snowflake Cloud...")
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE
        )

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='DAILY_WEATHER',
            auto_create_table=True
        )
        conn.close()
        if success:
            logging.info(f"Loading Successful! Loaded {nrows} rows to Snowflake Cloud.")
        else:
            logging.error("Failed to load data to Snowflake via write_pandas")

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
