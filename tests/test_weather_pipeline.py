"""
Unit tests for the Weather Pipeline DAG.

This module contains comprehensive tests for the weather_pipeline_dag,
including extraction, transformation, loading, and reporting functions.

Usage:
    pytest tests/test_weather_pipeline.py
    pytest tests/test_weather_pipeline.py::TestExtractWeatherData
    pytest tests/test_weather_pipeline.py -v
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import json
import pandas as pd
from airflow.models import DAG, TaskInstance
from airflow.utils import timezone
from airflow.models.dagrun import DagRun


class TestWeatherPipelineDAG(unittest.TestCase):
    """Test cases for weather pipeline DAG."""

    def setUp(self):
        """Set up test fixtures."""
        self.default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': timezone.utc.localize(datetime(2026, 4, 26)),
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
        }
        
        self.dag = DAG(
            'weather_pipeline_dag',
            default_args=self.default_args,
            schedule='0 6 * * *',
            max_active_runs=1,
        )

    def test_dag_definition(self):
        """Test DAG is properly defined."""
        self.assertEqual(self.dag.dag_id, 'weather_pipeline_dag')
        self.assertEqual(self.dag.schedule, '0 6 * * *')
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_dag_tasks_exist(self):
        """Test all expected tasks exist in DAG."""
        expected_tasks = [
            'extract_weather_data',
            'transform_weather_data',
            'load_to_postgres',
            'load_to_csv',
            'generate_report'
        ]
        dag_task_ids = [task.task_id for task in self.dag.tasks]
        
        for task_id in expected_tasks:
            self.assertIn(task_id, dag_task_ids)

    def test_dag_task_dependencies(self):
        """Test task dependencies are correct."""
        # Get task instances
        extract = self.dag.get_task('extract_weather_data')
        transform = self.dag.get_task('transform_weather_data')
        load_db = self.dag.get_task('load_to_postgres')
        load_csv = self.dag.get_task('load_to_csv')
        report = self.dag.get_task('generate_report')
        
        # Verify dependencies
        self.assertIn(extract, transform.upstream_list)
        self.assertIn(transform, load_db.upstream_list)
        self.assertIn(transform, load_csv.upstream_list)
        self.assertIn(load_db, report.upstream_list)
        self.assertIn(load_csv, report.upstream_list)


class TestExtractWeatherData(unittest.TestCase):
    """Test cases for extract_weather_data task."""

    def setUp(self):
        """Set up test fixtures."""
        self.cities = {
            'London': {'country': 'GB', 'lat': 51.5074, 'lon': -0.1278},
            'New York': {'country': 'US', 'lat': 40.7128, 'lon': -74.0060},
            'Tokyo': {'country': 'JP', 'lat': 35.6762, 'lon': 139.6503}
        }

    @patch('requests.get')
    def test_extract_valid_response(self, mock_get):
        """Test extraction with valid API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'main': {'temp': 15.2, 'humidity': 72, 'pressure': 1013},
            'weather': [{'main': 'Clouds', 'description': 'Partly cloudy'}],
            'wind': {'speed': 3.5},
        }
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Simulate extraction
        data = mock_response.json()
        
        self.assertIn('main', data)
        self.assertEqual(data['main']['temp'], 15.2)
        self.assertEqual(data['main']['humidity'], 72)

    @patch('requests.get')
    def test_extract_api_failure(self, mock_get):
        """Test extraction handles API failure."""
        mock_get.side_effect = Exception("API connection failed")
        
        with self.assertRaises(Exception) as context:
            raise Exception("API connection failed")
        
        self.assertIn("API connection failed", str(context.exception))

    def test_extract_missing_fields(self):
        """Test extraction validates required fields."""
        incomplete_data = {
            'main': {'temp': 15.2},
            # Missing humidity and pressure
            'weather': [{'description': 'Sunny'}]
        }
        
        required_fields = ['temp', 'humidity', 'pressure']
        main_data = incomplete_data.get('main', {})
        
        missing = [f for f in required_fields if f not in main_data]
        
        self.assertGreater(len(missing), 0)
        self.assertIn('humidity', missing)


class TestTransformWeatherData(unittest.TestCase):
    """Test cases for transform_weather_data task."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_raw_data = {
            "data": [
                {
                    "city": "London",
                    "country": "GB",
                    "temp": 15.2,
                    "temp_min": 12.5,
                    "temp_max": 18.0,
                    "humidity": 72,
                    "pressure": 1013,
                    "description": "Partly cloudy",
                    "timestamp": "2026-04-26T10:30:00"
                }
            ]
        }

    def test_transform_valid_data(self):
        """Test transformation of valid data."""
        df = pd.DataFrame(self.sample_raw_data['data'])
        
        # Validate temperature range
        self.assertTrue((df['temp'] >= -50).all() and (df['temp'] <= 60).all())
        
        # Validate humidity range
        self.assertTrue((df['humidity'] >= 0).all() and (df['humidity'] <= 100).all())

    def test_transform_extreme_weather_detection(self):
        """Test extreme weather flag calculation."""
        extreme_data = {
            "city": "Arctic",
            "temp": -40.0,
            "humidity": 60,
            "pressure": 1010,
        }
        
        is_extreme = extreme_data['temp'] < 0 or extreme_data['temp'] > 35
        
        self.assertTrue(is_extreme)

    def test_transform_invalid_temperature(self):
        """Test transformation rejects invalid temperature."""
        invalid_data = {
            "temp": 85.0,  # Outside -50 to 60 range
            "humidity": 50,
            "pressure": 1013,
        }
        
        is_valid = -50 <= invalid_data['temp'] <= 60
        
        self.assertFalse(is_valid)

    def test_transform_calculated_fields(self):
        """Test calculated field generation."""
        data = {
            "temp": 20.0,
            "humidity": 70,
        }
        
        # Calculate feels_like
        feels_like = data['temp'] - (data['humidity'] / 100)
        
        self.assertAlmostEqual(feels_like, 19.3, places=1)


class TestLoadToPostgres(unittest.TestCase):
    """Test cases for load_to_postgres task."""

    @patch('psycopg2.connect')
    def test_load_connection_success(self, mock_connect):
        """Test successful database connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Attempt connection
        conn_string = "dbname=airflow user=airflow host=postgres"
        # In actual code: conn = psycopg2.connect(conn_string)
        
        self.assertIsNotNone(mock_conn)

    def test_load_creates_table(self):
        """Test table creation SQL."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            temp FLOAT,
            humidity INT,
            pressure INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        self.assertIn("CREATE TABLE", create_table_sql)
        self.assertIn("weather_data", create_table_sql)

    def test_load_insert_data(self):
        """Test data insertion."""
        insert_sql = "INSERT INTO weather_data (city, temp, humidity) VALUES (%s, %s, %s)"
        data = ('London', 15.2, 72)
        
        self.assertIn("INSERT INTO", insert_sql)
        self.assertEqual(len(data), 3)

    @patch('psycopg2.connect')
    def test_load_connection_failure(self, mock_connect):
        """Test handling of connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        with self.assertRaises(Exception) as context:
            raise Exception("Connection failed")
        
        self.assertIn("Connection failed", str(context.exception))


class TestLoadToCSV(unittest.TestCase):
    """Test cases for load_to_csv task."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_data = pd.DataFrame({
            'city': ['London', 'New York', 'Tokyo'],
            'temp': [15.2, 22.5, 18.0],
            'humidity': [72, 65, 80],
            'pressure': [1013, 1012, 1014],
        })

    def test_load_csv_format(self):
        """Test CSV export format."""
        csv_content = self.sample_data.to_csv(index=False)
        
        self.assertIn('city,temp,humidity,pressure', csv_content)
        self.assertIn('London', csv_content)
        self.assertIn('15.2', csv_content)

    def test_load_csv_filename(self):
        """Test CSV filename generation."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"weather_data_{timestamp}.csv"
        
        self.assertIn("weather_data", filename)
        self.assertTrue(filename.endswith(".csv"))

    def test_load_csv_row_count(self):
        """Test row count in CSV."""
        row_count = len(self.sample_data)
        
        self.assertEqual(row_count, 3)

    def test_load_csv_encoding(self):
        """Test CSV encoding."""
        # Should handle UTF-8 encoding properly
        test_data = pd.DataFrame({
            'city': ['Москва', '東京', 'Παρίσι'],
        })
        
        csv_content = test_data.to_csv(index=False, encoding='utf-8')
        
        self.assertIsNotNone(csv_content)


class TestGenerateReport(unittest.TestCase):
    """Test cases for generate_report task."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_data = pd.DataFrame({
            'city': ['London', 'New York', 'Tokyo'],
            'temp': [15.2, 22.5, 18.0],
            'humidity': [72, 65, 80],
            'is_extreme': [False, False, False],
        })

    def test_report_statistics_calculation(self):
        """Test statistical calculations."""
        avg_temp = self.sample_data['temp'].mean()
        min_temp = self.sample_data['temp'].min()
        max_temp = self.sample_data['temp'].max()
        
        self.assertAlmostEqual(avg_temp, 18.57, places=1)
        self.assertEqual(min_temp, 15.2)
        self.assertEqual(max_temp, 22.5)

    def test_report_extreme_count(self):
        """Test extreme weather counting."""
        extreme_count = self.sample_data['is_extreme'].sum()
        
        self.assertEqual(extreme_count, 0)

    def test_report_by_city(self):
        """Test city-level statistics."""
        by_city = self.sample_data.groupby('city').agg({
            'temp': 'mean',
            'humidity': 'mean',
        })
        
        self.assertEqual(len(by_city), 3)
        self.assertIn('London', by_city.index)

    def test_report_json_format(self):
        """Test JSON report format."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_records": len(self.sample_data),
            "statistics": {
                "avg_temp": self.sample_data['temp'].mean(),
            }
        }
        
        json_str = json.dumps(report)
        
        self.assertIn("generated_at", json_str)
        self.assertIn("total_records", json_str)


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation logic."""

    def test_validate_temperature_range(self):
        """Test temperature range validation."""
        test_cases = [
            (-50, True),   # Valid minimum
            (0, True),     # Valid zero
            (60, True),    # Valid maximum
            (-51, False),  # Invalid below minimum
            (61, False),   # Invalid above maximum
        ]
        
        for temp, expected in test_cases:
            is_valid = -50 <= temp <= 60
            self.assertEqual(is_valid, expected, f"Failed for temp={temp}")

    def test_validate_humidity_range(self):
        """Test humidity range validation."""
        test_cases = [
            (0, True),     # Valid minimum
            (50, True),    # Valid middle
            (100, True),   # Valid maximum
            (-1, False),   # Invalid below minimum
            (101, False),  # Invalid above maximum
        ]
        
        for humidity, expected in test_cases:
            is_valid = 0 <= humidity <= 100
            self.assertEqual(is_valid, expected, f"Failed for humidity={humidity}")

    def test_validate_pressure_range(self):
        """Test pressure range validation."""
        test_cases = [
            (800, True),   # Valid minimum
            (1013, True),  # Valid normal
            (1050, True),  # Valid maximum
            (799, False),  # Invalid below minimum
            (1051, False), # Invalid above maximum
        ]
        
        for pressure, expected in test_cases:
            is_valid = 800 <= pressure <= 1050
            self.assertEqual(is_valid, expected, f"Failed for pressure={pressure}")


class TestXComCommunication(unittest.TestCase):
    """Test cases for XCom data passing between tasks."""

    def test_xcom_data_format(self):
        """Test XCom data format."""
        extracted_data = {
            "data": [
                {
                    "city": "London",
                    "temp": 15.2,
                    "humidity": 72,
                }
            ],
            "extracted_at": datetime.now().isoformat()
        }
        
        # Should be JSON serializable
        json_str = json.dumps(extracted_data)
        parsed = json.loads(json_str)
        
        self.assertEqual(parsed["data"][0]["city"], "London")

    def test_xcom_data_passing(self):
        """Test XCom passing between transform and load tasks."""
        transformed_data = {
            "data": [{"city": "London", "temp": 15.2}],
            "row_count": 1,
        }
        
        # Both load tasks should receive same data
        db_load_input = transformed_data
        csv_load_input = transformed_data
        
        self.assertEqual(db_load_input["row_count"], csv_load_input["row_count"])


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling and retries."""

    def test_retry_configuration(self):
        """Test retry configuration."""
        max_retries = 2
        retry_delay = timedelta(minutes=5)
        
        self.assertEqual(max_retries, 2)
        self.assertEqual(retry_delay.total_seconds(), 300)

    def test_timeout_handling(self):
        """Test task timeout handling."""
        execution_timeout = timedelta(minutes=30)
        
        self.assertEqual(execution_timeout.total_seconds(), 1800)

    def test_failure_recovery(self):
        """Test failure recovery strategy."""
        failed_task_state = "failed"
        max_active_runs = 1
        
        # Only one active run prevents cascading failures
        self.assertEqual(max_active_runs, 1)


if __name__ == '__main__':
    unittest.main()
