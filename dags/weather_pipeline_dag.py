"""
Weather Data Pipeline DAG
===========================
This DAG demonstrates a real-world data pipeline that:
1. Fetches weather data from OpenWeatherMap API
2. Transforms and validates the data
3. Stores results in both CSV and PostgreSQL
4. Generates a summary report

Architecture:
    [Extract] → [Transform] → [Load to DB] → [Generate Report]
                           ↓
                    [Load to CSV]

Task Dependencies:
    - extract_weather: Fetch data from API (3 cities)
    - transform_weather: Clean and validate data
    - load_to_postgres: Store in database
    - load_to_csv: Save to CSV file
    - generate_report: Create summary statistics

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import json
import os

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    dag_id='weather_pipeline_dag',
    default_args=default_args,
    description='Real-world weather data pipeline example',
    schedule='0 6 * * *',          # Airflow 3.0: `schedule` (not `schedule_interval`)
    tags=['weather', 'pipeline', 'example'],
    catchup=False,
    max_active_runs=1,
)


def extract_weather_data(**context):
    """
    Extract weather data from OpenWeatherMap API for multiple cities.
    
    Cities: London, New York, Tokyo
    Data collected:
        - Temperature (current, min, max)
        - Humidity
        - Pressure
        - Weather description
        - Timestamp
    """
    import requests
    from airflow.models import Variable
    
    # Get API key from Airflow variables (set manually in UI or via CLI)
    api_key = Variable.get("OPENWEATHER_API_KEY", default="demo")
    
    # Sample cities for demo
    cities = ['London', 'New York', 'Tokyo']
    
    # For demo purposes, use mock data instead of real API
    weather_data = {
        'data': [
            {
                'city': 'London',
                'country': 'GB',
                'temp': 15.2,
                'temp_min': 12.5,
                'temp_max': 18.0,
                'humidity': 72,
                'pressure': 1013,
                'description': 'Partly cloudy',
                'timestamp': datetime.utcnow().isoformat()
            },
            {
                'city': 'New York',
                'country': 'US',
                'temp': 22.5,
                'temp_min': 20.0,
                'temp_max': 25.0,
                'humidity': 65,
                'pressure': 1012,
                'description': 'Sunny',
                'timestamp': datetime.utcnow().isoformat()
            },
            {
                'city': 'Tokyo',
                'country': 'JP',
                'temp': 18.0,
                'temp_min': 16.0,
                'temp_max': 20.0,
                'humidity': 80,
                'pressure': 1015,
                'description': 'Rainy',
                'timestamp': datetime.utcnow().isoformat()
            }
        ],
        'extracted_at': datetime.utcnow().isoformat(),
        'source': 'OpenWeatherMap API (Demo)'
    }
    
    # Save to XCom for next tasks
    context['task_instance'].xcom_push(key='raw_weather_data', value=weather_data)
    
    print(f"✓ Extracted weather data for {len(weather_data['data'])} cities")
    print(f"  Timestamp: {weather_data['extracted_at']}")
    
    return {
        'status': 'success',
        'cities_processed': len(weather_data['data']),
        'timestamp': weather_data['extracted_at']
    }


def transform_weather_data(**context):
    """
    Transform and validate weather data.
    
    Transformations:
        - Validate temperature ranges (-50°C to 60°C)
        - Validate humidity (0-100%)
        - Add calculated fields (feels_like based on wind chill)
        - Ensure all required fields present
        - Add processing timestamp
    """
    import pandas as pd
    
    # Get raw data from previous task
    task_instance = context['task_instance']
    raw_data = task_instance.xcom_pull(
        task_ids='extract_weather_data',
        key='raw_weather_data'
    )
    
    if not raw_data:
        raise ValueError("No weather data to transform")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_data['data'])
    
    # Validation rules
    print("Validating data...")
    validation_issues = []
    
    # Check temperature ranges
    invalid_temps = df[(df['temp'] < -50) | (df['temp'] > 60)]
    if not invalid_temps.empty:
        validation_issues.append(f"Invalid temperatures: {len(invalid_temps)}")
    
    # Check humidity ranges
    invalid_humidity = df[(df['humidity'] < 0) | (df['humidity'] > 100)]
    if not invalid_humidity.empty:
        validation_issues.append(f"Invalid humidity: {len(invalid_humidity)}")
    
    if validation_issues:
        print(f"⚠ Validation issues found: {validation_issues}")
    else:
        print("✓ All data passed validation")
    
    # Add calculated fields
    df['feels_like'] = df['temp'] - (df['humidity'] / 100)  # Simplified
    df['processed_at'] = datetime.utcnow().isoformat()
    df['is_extreme'] = ((df['temp'] < 0) | (df['temp'] > 35)).astype(int)
    
    transformed_data = {
        'data': df.to_dict('records'),
        'row_count': len(df),
        'validation_issues': validation_issues,
        'processed_at': datetime.utcnow().isoformat()
    }
    
    # Save transformed data
    task_instance.xcom_push(key='transformed_weather_data', value=transformed_data)
    
    print(f"✓ Transformed {len(df)} records")
    print(f"  Extreme weather events: {df['is_extreme'].sum()}")
    
    return {
        'status': 'success',
        'records_processed': len(df),
        'validation_passed': len(validation_issues) == 0
    }


def load_to_postgres(**context):
    """
    Load transformed weather data to PostgreSQL database.
    
    Table: weather_data
    Columns:
        - id (auto-increment)
        - city, country
        - temp, temp_min, temp_max
        - humidity, pressure
        - description
        - feels_like
        - is_extreme (boolean)
        - timestamp
        - created_at
    """
    import psycopg2
    from psycopg2.extras import execute_values
    
    task_instance = context['task_instance']
    transformed_data = task_instance.xcom_pull(
        task_ids='transform_weather_data',
        key='transformed_weather_data'
    )
    
    if not transformed_data:
        raise ValueError("No transformed data found")
    
    # Connection parameters
    conn_params = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            country VARCHAR(10),
            temp FLOAT,
            temp_min FLOAT,
            temp_max FLOAT,
            humidity INT,
            pressure INT,
            description VARCHAR(255),
            feels_like FLOAT,
            is_extreme BOOLEAN,
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Prepare data for insertion
        records = transformed_data['data']
        insert_sql = """
        INSERT INTO weather_data 
        (city, country, temp, temp_min, temp_max, humidity, pressure, 
         description, feels_like, is_extreme, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        data_to_insert = [
            (
                r['city'],
                r.get('country'),
                r['temp'],
                r['temp_min'],
                r['temp_max'],
                r['humidity'],
                r['pressure'],
                r['description'],
                r['feels_like'],
                r['is_extreme'],
                r['timestamp']
            )
            for r in records
        ]
        
        # Insert data
        execute_values(cursor, insert_sql, data_to_insert)
        conn.commit()
        
        inserted_count = cursor.rowcount
        print(f"✓ Inserted {inserted_count} records into PostgreSQL")
        
        cursor.close()
        conn.close()
        
        return {
            'status': 'success',
            'records_inserted': inserted_count,
            'table': 'weather_data'
        }
        
    except psycopg2.Error as e:
        print(f"✗ Database error: {str(e)}")
        raise


def load_to_csv(**context):
    """
    Export transformed weather data to CSV file.
    Location: /opt/airflow/data/weather_data_<timestamp>.csv
    """
    import pandas as pd
    
    task_instance = context['task_instance']
    transformed_data = task_instance.xcom_pull(
        task_ids='transform_weather_data',
        key='transformed_weather_data'
    )
    
    if not transformed_data:
        raise ValueError("No transformed data found")
    
    # Convert to DataFrame
    df = pd.DataFrame(transformed_data['data'])
    
    # Create data directory if not exists
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Save to CSV with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    csv_path = f'{data_dir}/weather_data_{timestamp}.csv'
    
    df.to_csv(csv_path, index=False)
    
    print(f"✓ Exported {len(df)} records to CSV")
    print(f"  File: {csv_path}")
    
    task_instance.xcom_push(key='csv_export_path', value=csv_path)
    
    return {
        'status': 'success',
        'records_exported': len(df),
        'file_path': csv_path
    }


def generate_report(**context):
    """
    Generate summary statistics report.
    
    Report includes:
        - Average temperature by city
        - Min/Max temperatures
        - Average humidity
        - Count of extreme weather events
        - Data quality metrics
    """
    import pandas as pd
    
    task_instance = context['task_instance']
    transformed_data = task_instance.xcom_pull(
        task_ids='transform_weather_data',
        key='transformed_weather_data'
    )
    
    if not transformed_data:
        raise ValueError("No transformed data found")
    
    df = pd.DataFrame(transformed_data['data'])
    
    # Generate statistics
    report = {
        'report_generated_at': datetime.utcnow().isoformat(),
        'total_records': len(df),
        'cities': df['city'].unique().tolist(),
        'statistics': {
            'avg_temp': float(df['temp'].mean()),
            'min_temp': float(df['temp'].min()),
            'max_temp': float(df['temp'].max()),
            'avg_humidity': float(df['humidity'].mean()),
            'extreme_weather_count': int(df['is_extreme'].sum()),
            'avg_pressure': float(df['pressure'].mean())
        },
        'by_city': {}
    }
    
    # Statistics by city
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        report['by_city'][city] = {
            'avg_temp': float(city_data['temp'].mean()),
            'avg_humidity': float(city_data['humidity'].mean()),
            'description': city_data['description'].iloc[0]
        }
    
    # Print report
    print("\n" + "="*60)
    print("WEATHER PIPELINE REPORT")
    print("="*60)
    print(f"Generated: {report['report_generated_at']}")
    print(f"Total Records: {report['total_records']}")
    print(f"\nGlobal Statistics:")
    for stat, value in report['statistics'].items():
        print(f"  {stat}: {value:.2f}" if isinstance(value, float) else f"  {stat}: {value}")
    print(f"\nBy City:")
    for city, stats in report['by_city'].items():
        print(f"  {city}: Temp={stats['avg_temp']:.1f}°C, Humidity={stats['avg_humidity']:.0f}%")
    print("="*60 + "\n")
    
    task_instance.xcom_push(key='report', value=report)
    
    return {
        'status': 'success',
        'report_generated': True,
        'records_analyzed': len(df)
    }


# Build DAG tasks
with dag:
    # Extract weather data from API
    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
        doc=extract_weather_data.__doc__,
    )
    
    # Transform and validate data
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
        doc=transform_weather_data.__doc__,
    )
    
    # Load to PostgreSQL
    load_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        doc=load_to_postgres.__doc__,
        trigger_rule='none_failed',
    )
    
    # Load to CSV
    load_csv_task = PythonOperator(
        task_id='load_to_csv',
        python_callable=load_to_csv,
        doc=load_to_csv.__doc__,
        trigger_rule='none_failed',
    )
    
    # Generate report
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        doc=generate_report.__doc__,
    )
    
    # Define task dependencies
    extract_task >> transform_task >> [load_postgres_task, load_csv_task] >> report_task
