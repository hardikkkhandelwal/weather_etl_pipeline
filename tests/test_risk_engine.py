import sys
import os
from unittest.mock import MagicMock

# =====================================================================
# Advanced Meta-Programming Mock for nested Airflow TaskFlow testing
# =====================================================================
MOCKED_TASKS = {}

class TaskMock:
    def __init__(self, func):
        self.__wrapped__ = func
        self.__name__ = func.__name__
        
    def __call__(self, *args, **kwargs):
        # Prevent running actual function code during DAG parsing, return task mock
        return self
        
    def __rshift__(self, other):
        # Support Task1 >> Task2 and Task1 >> [Task2, Task3]
        return other
        
    def __lshift__(self, other):
        return other

class MockAirflowDecorators:
    @staticmethod
    def dag(*args, **kwargs):
        def decorator(func):
            # Return a wrapper that executes the DAG function to register nested tasks
            def dag_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return dag_wrapper
        return decorator

    @staticmethod
    def task(*args, **kwargs):
        def decorator(func):
            # Register the nested function in the global registry for test access
            MOCKED_TASKS[func.__name__] = func
            return TaskMock(func)
            
        # Handle both @task and @task(...) syntax
        if len(args) == 1 and callable(args[0]):
            return decorator(args[0])
        return decorator

mock_decorators = MagicMock()
mock_decorators.dag = MockAirflowDecorators.dag
mock_decorators.task = MockAirflowDecorators.task
sys.modules['airflow.decorators'] = mock_decorators

mock_hooks = MagicMock()
mock_hooks.BaseHook = MagicMock()
sys.modules['airflow.hooks'] = MagicMock()
sys.modules['airflow.hooks.base'] = mock_hooks

# =====================================================================
# Now import the DAG and run the unit tests
# =====================================================================
import unittest

# Adjust path to import functions from the DAG file
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'dags'))
from logistics_risk_etl_dag import supply_chain_logistics_weather_risk_pipeline

class TestLogisticsRiskEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Execute the DAG pipeline function once to trigger task definitions and register them
        supply_chain_logistics_weather_risk_pipeline()

    def setUp(self):
        # Retrieve the registered inner task from the meta-registry
        self.risk_engine = MOCKED_TASKS.get('transform_risk_engine')
        if not self.risk_engine:
            raise AttributeError("Failed to extract transform_risk_engine from Airflow DAG closure.")

    def test_clear_weather_low_risk(self):
        """Test that normal clear weather conditions result in low risk score (10)."""
        forecasts = [
            {
                "forecast_id": "LON_2026-05-23",
                "hub_id": "LON",
                "forecast_date": "2026-05-23",
                "max_temp_c": 20.0,
                "min_temp_c": 12.0,
                "max_wind_speed_kmh": 15.0,
                "precipitation_probability": 10.0,
                "weather_code": 1  # Clear
            }
        ]
        shipments = [
            {
                "shipment_id": "SHIP-TEST-1",
                "cargo_type": "GENERAL_MERCHANDISE",
                "cargo_value_usd": 100000.0,
                "origin_hub_id": "LON",
                "destination_hub_id": "LON",
                "departure_time": "2026-05-23T08:00:00",
                "arrival_time": "2026-05-23T18:00:00"
            }
        ]
        
        result = self.risk_engine(forecasts, shipments)
        alert = result["alerts"][0]
        
        self.assertEqual(alert["risk_level"], "LOW")
        self.assertEqual(alert["route_risk_score"], 10.0)
        self.assertIn("No severe meteorological risk factors detected", alert["risk_reason"])

    def test_pharma_freezing_high_risk(self):
        """Test that freezing temperatures at route points flag high risk (+60) for Pharmaceuticals."""
        forecasts = [
            {
                "forecast_id": "LON_2026-05-23",
                "hub_id": "LON",
                "forecast_date": "2026-05-23",
                "max_temp_c": 5.0,
                "min_temp_c": -1.5,  # Freezing
                "max_wind_speed_kmh": 10.0,
                "precipitation_probability": 5.0,
                "weather_code": 1
            }
        ]
        shipments = [
            {
                "shipment_id": "SHIP-TEST-2",
                "cargo_type": "PHARMACEUTICALS",
                "cargo_value_usd": 1500000.0,
                "origin_hub_id": "LON",
                "destination_hub_id": "LON",
                "departure_time": "2026-05-23T08:00:00",
                "arrival_time": "2026-05-23T18:00:00"
            }
        ]
        
        result = self.risk_engine(forecasts, shipments)
        alert = result["alerts"][0]
        
        self.assertEqual(alert["risk_level"], "HIGH")
        self.assertEqual(alert["route_risk_score"], 70.0)  # 10 base + 60 freezing
        self.assertIn("ruins Pharmaceuticals", alert["risk_reason"])

    def test_electronics_moisture_medium_risk(self):
        """Test that heavy rainfall probabilities at route points flag medium risk (+40) for Electronics."""
        forecasts = [
            {
                "forecast_id": "MUM_2026-05-23",
                "hub_id": "MUM",
                "forecast_date": "2026-05-23",
                "max_temp_c": 30.0,
                "min_temp_c": 26.0,
                "max_wind_speed_kmh": 15.0,
                "precipitation_probability": 90.0,  # Extreme precipitation
                "weather_code": 3
            }
        ]
        shipments = [
            {
                "shipment_id": "SHIP-TEST-3",
                "cargo_type": "ELECTRONICS",
                "cargo_value_usd": 500000.0,
                "origin_hub_id": "MUM",
                "destination_hub_id": "MUM",
                "departure_time": "2026-05-23T08:00:00",
                "arrival_time": "2026-05-23T18:00:00"
            }
        ]
        
        result = self.risk_engine(forecasts, shipments)
        alert = result["alerts"][0]
        
        self.assertEqual(alert["risk_level"], "MEDIUM")
        self.assertEqual(alert["route_risk_score"], 50.0)  # 10 base + 40 precipitation
        self.assertIn("poses water damage risk to Electronics", alert["risk_reason"])

    def test_extreme_wind_delay_risk(self):
        """Test that high wind speeds flag medium/high risk (+45) for all general cargo."""
        forecasts = [
            {
                "forecast_id": "TYO_2026-05-23",
                "hub_id": "TYO",
                "forecast_date": "2026-05-23",
                "max_temp_c": 18.0,
                "min_temp_c": 14.0,
                "max_wind_speed_kmh": 55.0,  # Gale winds
                "precipitation_probability": 20.0,
                "weather_code": 1
            }
        ]
        shipments = [
            {
                "shipment_id": "SHIP-TEST-4",
                "cargo_type": "GENERAL_MERCHANDISE",
                "cargo_value_usd": 80000.0,
                "origin_hub_id": "TYO",
                "destination_hub_id": "TYO",
                "departure_time": "2026-05-23T08:00:00",
                "arrival_time": "2026-05-23T18:00:00"
            }
        ]
        
        result = self.risk_engine(forecasts, shipments)
        alert = result["alerts"][0]
        
        self.assertEqual(alert["risk_level"], "MEDIUM")
        self.assertEqual(alert["route_risk_score"], 55.0)  # 10 base + 45 wind
        self.assertIn("High Wind speeds", alert["risk_reason"])

if __name__ == '__main__':
    unittest.main()
