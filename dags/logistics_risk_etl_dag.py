import logging
import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

# Default configuration arguments for the orchestration engine
default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 20),
    catchup=False,
    tags=['supply_chain', 'logistics', 'risk_engine', 'snowflake'],
)
def supply_chain_logistics_weather_risk_pipeline():
    
    def get_snowflake_engine():
        """Establishes connection to Snowflake using secure Airflow connection hooks."""
        conn = BaseHook.get_connection('snowflake_creds')
        
        # Robust parsing in case database is in the schema path (e.g. 'WEATHER_DB/WEATHER_SCHEMA')
        db = conn.extra_dejson.get('database')
        schema = conn.schema
        if not db and schema and '/' in schema:
            parts = schema.split('/')
            db = parts[0]
            schema = parts[1]
            
        engine = create_engine(
            URL(
                user=conn.login,
                password=conn.password,
                account=conn.host,
                database=db,
                schema=schema,
                warehouse=conn.extra_dejson.get('warehouse')
            )
        )
        return engine

    @task
    def initialize_snowflake_schema():
        """
        Reads the enterprise DDL SQL script and executes it in Snowflake.
        This guarantees that all Dimension and Fact tables are built and seeded properly.
        """
        logging.info("Initializing Snowflake Schema and Seeding Dimension Tables...")
        
        # Locate the setup DDL script mounted in the dags folder
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(dag_dir, 'snowflake_setup', 'setup_star_schema.sql')
        
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"DDL script not found at expected path: {sql_file_path}")
            
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()
            
        # Parse individual SQL statements (handling potential comments and empty lines)
        statements = []
        current_statement = []
        for line in sql_script.split('\n'):
            # Strip inline comments to prevent commenting out subsequent elements when joining
            if '--' in line:
                line = line.split('--', 1)[0]
            stripped = line.strip()
            if not stripped:
                continue
            current_statement.append(line)
            if stripped.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
                
        engine = get_snowflake_engine()
        with engine.begin() as connection:
            for statement in statements:
                if statement.strip():
                    logging.info(f"Executing SQL Statement: {statement[:60]}...")
                    connection.execute(text(statement))
                    
        logging.info("Snowflake Star Schema initialized and Seed data updated successfully!")

    @task
    def extract_weather_forecasts() -> list:
        """
        Extracts 5-day weather forecasts for all global logistics hubs defined in the pipeline.
        Fetches: max/min temperature, precipitation probability, wind speed, and weather code.
        """
        logging.info("Beginning Extraction of 5-Day Weather Forecasts for Global Hubs...")
        url = "https://api.open-meteo.com/v1/forecast"
        
        hubs = {
            "MUM": {"lat": 19.0760, "lon": 72.8777},
            "NYC": {"lat": 40.7128, "lon": -74.0060},
            "LON": {"lat": 51.5074, "lon": -0.1278},
            "TYO": {"lat": 35.6762, "lon": 139.6503},
            "SYD": {"lat": -33.8688, "lon": 151.2093},
            "PAR": {"lat": 48.8566, "lon": 2.3522},
            "BER": {"lat": 52.5200, "lon": 13.4050},
            "DXB": {"lat": 25.2048, "lon": 55.2708},
            "SIN": {"lat": 1.3521, "lon": 103.8198},
            "LAX": {"lat": 34.0522, "lon": -118.2437}
        }
        
        all_forecast_records = []
        
        for hub_id, coords in hubs.items():
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "daily": "temperature_2m_max,temperature_2m_min,wind_speed_10m_max,precipitation_probability_max,weather_code",
                "timezone": "auto",
                "forecast_days": 5
            }
            
            try:
                logging.info(f"Extracting forecast data for hub: {hub_id}")
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                daily_data = data['daily']
                for i in range(len(daily_data['time'])):
                    forecast_record = {
                        "forecast_id": f"{hub_id}_{daily_data['time'][i]}",
                        "hub_id": hub_id,
                        "forecast_date": daily_data['time'][i],
                        "max_temp_c": daily_data['temperature_2m_max'][i],
                        "min_temp_c": daily_data['temperature_2m_min'][i],
                        "max_wind_speed_kmh": daily_data['wind_speed_10m_max'][i],
                        "precipitation_probability": daily_data['precipitation_probability_max'][i],
                        "weather_code": daily_data['weather_code'][i]
                    }
                    all_forecast_records.append(forecast_record)
                    
                logging.info(f"Successfully extracted 5-day forecast for hub {hub_id}")
            except Exception as e:
                logging.error(f"Failed to fetch forecast for hub {hub_id}: {e}")
                continue
                
        logging.info(f"Forecast extraction complete. Total records collected: {len(all_forecast_records)}")
        return all_forecast_records

    @task
    def extract_shipment_schedule() -> list:
        """
        Extracts internal cargo shipment schedules from the operational operational JSON feed.
        """
        logging.info("Extracting Shipment Schedules from Internal Operational Feed...")
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        json_file_path = os.path.join(dag_dir, 'data', 'shipments_schedule.json')
        
        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"Shipment JSON file not found at: {json_file_path}")
            
        with open(json_file_path, 'r') as file:
            shipments = json.load(file)
            
        logging.info(f"Successfully loaded {len(shipments)} cargo shipments from feed.")
        return shipments

    @task
    def transform_risk_engine(forecast_records: list, shipment_records: list) -> dict:
        """
        Core Transformation Phase: The Supply Chain Logistics Risk Engine.
        Merges shipment routes with forecast data and runs vectorized rules to calculate
        the route_risk_score (0-100) and risk_level (LOW, MEDIUM, HIGH) for each shipment.
        """
        logging.info("Executing Logistics Route Risk Calculations...")
        
        df_forecast = pd.DataFrame(forecast_records)
        df_shipment = pd.DataFrame(shipment_records)
        
        # Parse times and extract dates to join shipments and forecasts correctly
        df_shipment['DEPARTURE_DATE'] = pd.to_datetime(df_shipment['departure_time']).dt.strftime('%Y-%m-%d')
        df_shipment['ARRIVAL_DATE'] = pd.to_datetime(df_shipment['arrival_time']).dt.strftime('%Y-%m-%d')
        
        calculated_alerts = []
        
        # Evaluate risk for each scheduled shipment
        for _, shipment in df_shipment.iterrows():
            shipment_id = shipment['shipment_id']
            cargo_type = shipment['cargo_type']
            origin = shipment['origin_hub_id']
            dest = shipment['destination_hub_id']
            dep_date = shipment['DEPARTURE_DATE']
            arr_date = shipment['ARRIVAL_DATE']
            
            # Fetch weather forecast at Origin on departure day
            origin_weather = df_forecast[
                (df_forecast['hub_id'] == origin) & (df_forecast['forecast_date'] == dep_date)
            ]
            # Fetch weather forecast at Destination on arrival day
            dest_weather = df_forecast[
                (df_forecast['hub_id'] == dest) & (df_forecast['forecast_date'] == arr_date)
            ]
            
            # Use Unique Threat Vector Activation scoring to prevent double counting
            threats = {
                "FREEZING": 0.0,
                "HEAT": 0.0,
                "PRECIPITATION": 0.0,
                "WIND": 0.0,
                "SEVERE": 0.0
            }
            risk_reasons = []
            
            # Evaluate origin threats
            if not origin_weather.empty:
                o_row = origin_weather.iloc[0]
                
                if cargo_type == 'PHARMACEUTICALS' and o_row['min_temp_c'] < 2.0:
                    threats["FREEZING"] = max(threats["FREEZING"], 60.0)
                    risk_reasons.append(f"Origin Freezing Temp ({o_row['min_temp_c']}°C) ruins Pharmaceuticals.")
                    
                if cargo_type == 'PHARMACEUTICALS' and o_row['max_temp_c'] > 35.0:
                    threats["HEAT"] = max(threats["HEAT"], 50.0)
                    risk_reasons.append(f"Origin Extreme Heat ({o_row['max_temp_c']}°C) ruins Pharmaceuticals.")
                    
                if cargo_type == 'ELECTRONICS' and o_row['precipitation_probability'] > 75.0:
                    threats["PRECIPITATION"] = max(threats["PRECIPITATION"], 40.0)
                    risk_reasons.append(f"Origin Storm Warning (Precipitation Prob {o_row['precipitation_probability']}%) poses water damage risk to Electronics.")
                    
                if o_row['max_wind_speed_kmh'] > 40.0:
                    threats["WIND"] = max(threats["WIND"], 45.0)
                    risk_reasons.append(f"High Wind speeds ({o_row['max_wind_speed_kmh']} km/h) at origin {origin} may ground aircraft or stall container cranes.")
                    
                if o_row['weather_code'] in [71, 73, 75, 85, 86, 95, 96, 99]:
                    threats["SEVERE"] = max(threats["SEVERE"], 30.0)
                    risk_reasons.append(f"Severe weather code ({int(o_row['weather_code'])}) at origin {origin}.")

            # Evaluate destination threats
            if not dest_weather.empty:
                d_row = dest_weather.iloc[0]
                
                if cargo_type == 'PHARMACEUTICALS' and d_row['min_temp_c'] < 2.0:
                    threats["FREEZING"] = max(threats["FREEZING"], 60.0)
                    risk_reasons.append(f"Destination Freezing Temp ({d_row['min_temp_c']}°C) ruins Pharmaceuticals.")
                    
                if cargo_type == 'PHARMACEUTICALS' and d_row['max_temp_c'] > 35.0:
                    threats["HEAT"] = max(threats["HEAT"], 50.0)
                    risk_reasons.append(f"Destination Extreme Heat ({d_row['max_temp_c']}°C) ruins Pharmaceuticals.")
                    
                if cargo_type == 'ELECTRONICS' and d_row['precipitation_probability'] > 75.0:
                    threats["PRECIPITATION"] = max(threats["PRECIPITATION"], 40.0)
                    risk_reasons.append(f"Destination Storm Warning (Precipitation Prob {d_row['precipitation_probability']}%) poses water damage risk to Electronics.")
                    
                if d_row['max_wind_speed_kmh'] > 40.0:
                    threats["WIND"] = max(threats["WIND"], 45.0)
                    risk_reasons.append(f"High Wind speeds ({d_row['max_wind_speed_kmh']} km/h) at destination {dest} may stall airport/port logistics.")
                    
                if d_row['weather_code'] in [71, 73, 75, 85, 86, 95, 96, 99]:
                    threats["SEVERE"] = max(threats["SEVERE"], 30.0)
                    risk_reasons.append(f"Severe weather code ({int(d_row['weather_code'])}) at destination {dest}.")

            # Final risk score calculation (base line 10.0 + unique threat elements)
            final_risk_score = 10.0 + sum(threats.values())
            final_risk_score = min(final_risk_score, 100.0)
            
            # Map score to risk levels
            if final_risk_score >= 60.0:
                risk_level = 'HIGH'
            elif final_risk_score >= 30.0:
                risk_level = 'MEDIUM'
            else:
                risk_level = 'LOW'
                
            reason_text = "; ".join(risk_reasons) if risk_reasons else "No severe meteorological risk factors detected."
            
            calculated_alerts.append({
                "alert_id": f"{shipment_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "shipment_id": shipment_id,
                "route_risk_score": final_risk_score,
                "risk_level": risk_level,
                "risk_reason": reason_text,
                "evaluated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
        logging.info(f"Logistics transformation complete. Evaluated {len(calculated_alerts)} routes.")
        
        # Pack data to return to subsequent load tasks
        return {
            "forecasts": forecast_records,
            "shipments": shipment_records,
            "alerts": calculated_alerts
        }

    @task
    def load_data_to_snowflake(pipeline_data: dict):
        """
        Loads the structured data into respective Snowflake Dimensions and Facts.
        Uses advanced UPSERT operations (MERGE) for clean, idempotent execution.
        """
        logging.info("Starting Bulk Load to Snowflake Cloud Warehouse...")
        engine = get_snowflake_engine()
        
        df_forecasts = pd.DataFrame(pipeline_data["forecasts"])
        df_shipments = pd.DataFrame(pipeline_data["shipments"])
        df_alerts = pd.DataFrame(pipeline_data["alerts"])
        
        with engine.begin() as connection:
            # 1. Update/Insert DIM_SHIPMENTS (Dimensional Table)
            logging.info("Loading DIM_SHIPMENTS table...")
            for _, shipment in df_shipments.iterrows():
                merge_shipment_sql = f"""
                MERGE INTO DIM_SHIPMENTS AS target
                USING (
                    SELECT 
                        '{shipment['shipment_id']}' AS SHIPMENT_ID,
                        '{shipment['cargo_type']}' AS CARGO_TYPE,
                        {shipment['cargo_value_usd']} AS CARGO_VALUE_USD,
                        '{shipment['origin_hub_id']}' AS ORIGIN_HUB_ID,
                        '{shipment['destination_hub_id']}' AS DESTINATION_HUB_ID,
                        '{shipment['departure_time']}'::TIMESTAMP_NTZ AS DEPARTURE_TIME,
                        '{shipment['arrival_time']}'::TIMESTAMP_NTZ AS ARRIVAL_TIME
                ) AS source
                ON target.SHIPMENT_ID = source.SHIPMENT_ID
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.CARGO_TYPE = source.CARGO_TYPE,
                        target.CARGO_VALUE_USD = source.CARGO_VALUE_USD,
                        target.ORIGIN_HUB_ID = source.ORIGIN_HUB_ID,
                        target.DESTINATION_HUB_ID = source.DESTINATION_HUB_ID,
                        target.DEPARTURE_TIME = source.DEPARTURE_TIME,
                        target.ARRIVAL_TIME = source.ARRIVAL_TIME
                WHEN NOT MATCHED THEN
                    INSERT (SHIPMENT_ID, CARGO_TYPE, CARGO_VALUE_USD, ORIGIN_HUB_ID, DESTINATION_HUB_ID, DEPARTURE_TIME, ARRIVAL_TIME)
                    VALUES (source.SHIPMENT_ID, source.CARGO_TYPE, source.CARGO_VALUE_USD, source.ORIGIN_HUB_ID, source.DESTINATION_HUB_ID, source.DEPARTURE_TIME, source.ARRIVAL_TIME);
                """
                connection.execute(text(merge_shipment_sql))
            
            # 2. Update/Insert FACT_WEATHER_FORECAST (Fact Table)
            logging.info("Loading FACT_WEATHER_FORECAST table...")
            for _, forecast in df_forecasts.iterrows():
                # Handling None / NaN in fields
                max_temp = 'NULL' if pd.isna(forecast['max_temp_c']) else forecast['max_temp_c']
                min_temp = 'NULL' if pd.isna(forecast['min_temp_c']) else forecast['min_temp_c']
                wind_speed = 'NULL' if pd.isna(forecast['max_wind_speed_kmh']) else forecast['max_wind_speed_kmh']
                precip = 'NULL' if pd.isna(forecast['precipitation_probability']) else forecast['precipitation_probability']
                weather_code = 'NULL' if pd.isna(forecast['weather_code']) else int(forecast['weather_code'])

                merge_forecast_sql = f"""
                MERGE INTO FACT_WEATHER_FORECAST AS target
                USING (
                    SELECT 
                        '{forecast['forecast_id']}' AS FORECAST_ID,
                        '{forecast['hub_id']}' AS HUB_ID,
                        '{forecast['forecast_date']}'::DATE AS FORECAST_DATE,
                        {max_temp} AS MAX_TEMP_C,
                        {min_temp} AS MIN_TEMP_C,
                        {wind_speed} AS MAX_WIND_SPEED_KMH,
                        {precip} AS PRECIPITATION_PROBABILITY,
                        {weather_code} AS WEATHER_CODE
                ) AS source
                ON target.FORECAST_ID = source.FORECAST_ID
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.MAX_TEMP_C = source.MAX_TEMP_C,
                        target.MIN_TEMP_C = source.MIN_TEMP_C,
                        target.MAX_WIND_SPEED_KMH = source.MAX_WIND_SPEED_KMH,
                        target.PRECIPITATION_PROBABILITY = source.PRECIPITATION_PROBABILITY,
                        target.WEATHER_CODE = source.WEATHER_CODE,
                        target.PROCESSED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (FORECAST_ID, HUB_ID, FORECAST_DATE, MAX_TEMP_C, MIN_TEMP_C, MAX_WIND_SPEED_KMH, PRECIPITATION_PROBABILITY, WEATHER_CODE, PROCESSED_AT)
                    VALUES (source.FORECAST_ID, source.HUB_ID, source.FORECAST_DATE, source.MAX_TEMP_C, source.MIN_TEMP_C, source.MAX_WIND_SPEED_KMH, source.PRECIPITATION_PROBABILITY, source.WEATHER_CODE, CURRENT_TIMESTAMP());
                """
                connection.execute(text(merge_forecast_sql))

            # 3. Insert FACT_SHIPMENT_RISK_ALERTS (Fact Table)
            logging.info("Loading FACT_SHIPMENT_RISK_ALERTS table...")
            for _, alert in df_alerts.iterrows():
                # Clean up reasoning string for safe SQL execution
                escaped_reason = alert['risk_reason'].replace("'", "''")
                
                merge_alert_sql = f"""
                MERGE INTO FACT_SHIPMENT_RISK_ALERTS AS target
                USING (
                    SELECT 
                        '{alert['alert_id']}' AS ALERT_ID,
                        '{alert['shipment_id']}' AS SHIPMENT_ID,
                        {alert['route_risk_score']} AS ROUTE_RISK_SCORE,
                        '{alert['risk_level']}' AS RISK_LEVEL,
                        '{escaped_reason}' AS RISK_REASON,
                        '{alert['evaluated_at']}'::TIMESTAMP_NTZ AS EVALUATED_AT
                ) AS source
                ON target.ALERT_ID = source.ALERT_ID
                WHEN NOT MATCHED THEN
                    INSERT (ALERT_ID, SHIPMENT_ID, ROUTE_RISK_SCORE, RISK_LEVEL, RISK_REASON, EVALUATED_AT)
                    VALUES (source.ALERT_ID, source.SHIPMENT_ID, source.ROUTE_RISK_SCORE, source.RISK_LEVEL, source.RISK_REASON, source.EVALUATED_AT);
                """
                connection.execute(text(merge_alert_sql))
                
        logging.info("Bulk Data Load to Snowflake star schema completed successfully!")

    @task
    def generate_operational_alerts(pipeline_data: dict):
        """
        Downstream task simulating Slack/PagerDuty notification triggers.
        Filters high-risk shipments and outputs an operational warning file.
        """
        logging.info("Generating Logistics Warning Reports for High Risk Shipments...")
        df_alerts = pd.DataFrame(pipeline_data["alerts"])
        df_shipments = pd.DataFrame(pipeline_data["shipments"])
        
        # Merge alert results with shipment values for economic impact context
        df_merged = pd.merge(df_alerts, df_shipments, on='shipment_id')
        
        high_risk_incidents = df_merged[df_merged['risk_level'] == 'HIGH']
        
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        alerts_dir = os.path.join(dag_dir, 'alerts')
        os.makedirs(alerts_dir, exist_ok=True)
        
        output_file_path = os.path.join(alerts_dir, 'high_risk_shipments.json')
        
        if not high_risk_incidents.empty:
            logging.warning(f"CRITICAL DISRUPTION: Found {len(high_risk_incidents)} HIGH-RISK cargo routes!")
            
            high_risk_list = high_risk_incidents[[
                'shipment_id', 'cargo_type', 'cargo_value_usd', 
                'origin_hub_id', 'destination_hub_id', 'route_risk_score', 'risk_reason'
            ]].to_dict(orient='records')
            
            alert_payload = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "severity": "CRITICAL",
                "alert_count": len(high_risk_list),
                "total_cargo_value_at_risk_usd": float(high_risk_incidents['cargo_value_usd'].sum()),
                "incidents": high_risk_list
            }
            
            with open(output_file_path, 'w') as file:
                json.dump(alert_payload, file, indent=4)
                
            logging.info(f"Operational Incident Response Report written to: {output_file_path}")
        else:
            logging.info("Clear route analysis. No shipments flagged with critical risk levels.")
            
            empty_payload = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "severity": "CLEAR",
                "alert_count": 0,
                "total_cargo_value_at_risk_usd": 0.0,
                "incidents": []
            }
            with open(output_file_path, 'w') as file:
                json.dump(empty_payload, file, indent=4)
                
            logging.info("Clear operational alert file generated.")

    # Orchestrator Task Flow Definition
    schema_init = initialize_snowflake_schema()
    forecasts = extract_weather_forecasts()
    shipments = extract_shipment_schedule()
    
    # Calculate risks only after weather and shipment metadata are retrieved
    calculated_risk = transform_risk_engine(forecasts, shipments)
    
    # Run the load and alert tasks in parallel following the transformation
    load_snowflake = load_data_to_snowflake(calculated_risk)
    alert_operation = generate_operational_alerts(calculated_risk)
    
    # Define dependency boundaries
    schema_init >> [forecasts, shipments]
    calculated_risk >> [load_snowflake, alert_operation]

# Instantiate the pipeline
supply_chain_logistics_weather_risk_pipeline()
