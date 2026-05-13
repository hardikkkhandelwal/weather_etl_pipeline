# Global Weather ETL Pipeline 🌍⛅

An automated, containerized Data Engineering pipeline that extracts real-time weather data for 10 global cities, transforms it using Pandas, and loads it into a Snowflake cloud data warehouse. Orchestrated via Apache Airflow.

## 🛠️ Architecture & Tech Stack
* **Python**: Core scripting and data transformation (Pandas).
* **Apache Airflow**: Workflow orchestration (TaskFlow API) and scheduling (`@daily`).
* **Docker**: Containerization to run the Airflow cluster locally with custom dependencies.
* **Snowflake**: Cloud Data Warehouse for final storage.
* **Open-Meteo API**: Source of real-time weather data.

## 🚀 Features
* **Fault Tolerant Extraction**: Fetches data for 10 distinct cities. If the API fails for one city, the pipeline logs the error and gracefully continues fetching the rest.
* **Secure Secrets Management**: Database credentials are not hardcoded. They are securely fetched at runtime using Airflow's native `BaseHook` connection manager.
* **Modern Airflow Paradigms**: Utilizes the modern TaskFlow API (`@dag`, `@task`) for automatic XCom data passing between tasks.

## 📂 Project Structure
* `dags/weather_etl_dag.py`: The core Airflow DAG defining the Extract, Transform, and Load tasks.
* `Dockerfile`: Custom image definition to install `pandas` and `snowflake-sqlalchemy` onto the Airflow workers.
* `docker-compose.yaml`: Configuration to spin up the local Airflow cluster (Postgres, Redis, Webserver, Scheduler).
* `requirements.txt`: Python package dependencies.
* `etl_pipeline.py`: The original, standalone Python script used for prototyping before orchestration.

## ⚙️ How to Run
1. Clone the repository.
2. Ensure Docker Desktop is running.
3. Build and start the Airflow cluster:
   ```bash
   docker-compose up -d --build
   ```
4. Access the Airflow UI at `http://localhost:8080` (login: `airflow` / `airflow`).
5. Configure a Generic Connection named `snowflake_creds` in the Airflow UI with your Snowflake database details.
6. Unpause and trigger the `weather_etl_pipeline` DAG!
