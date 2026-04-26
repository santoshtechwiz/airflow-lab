"""
HTTP Weather DAG — Learning Example
=====================================

PURPOSE
-------
This DAG is a hands-on demonstration of Airflow's HTTP provider.
It shows how to talk to external web services from an Airflow pipeline
using two different tools:

  • HttpSensor   — Waits (polls) until an HTTP endpoint responds before
                   proceeding.  Great for "gate-keeping" your pipeline
                   when you depend on an external service being available.

  • HttpOperator — Makes a single HTTP request (GET, POST, etc.) and
                   stores the response in XCom so downstream tasks can
                   use the data.

FREE APIS USED (no signup, no API key needed)
---------------------------------------------
  1. wttr.in          — https://wttr.in/:city?format=j1
     Simple weather service.  Returns JSON with current conditions,
     temperature, wind, and more for any city name.

  2. Open-Meteo       — https://api.open-meteo.com/v1/forecast
     Free, open-source weather forecast API.  Returns current + hourly
     + daily forecasts.  No key, no rate-limit for moderate use.

AIRFLOW CONCEPTS DEMONSTRATED
------------------------------
  1. HttpSensor       — polling with poke_interval and timeout
  2. HttpOperator     — GET request with endpoint, http_conn_id, response_filter
  3. Connections      — http_conn_id maps to host/schema registered in Airflow UI
                        (registered by airflow-init in docker-compose.yml)
  4. XCom             — automatic (HttpOperator pushes return value) and
                        manual (xcom_push / xcom_pull in PythonOperator)
  5. response_filter  — lambda that transforms the raw response before XCom
  6. TaskGroup        — logical grouping of tasks in the UI
  7. BranchPythonOperator — conditional path based on temperature value

PIPELINE FLOW
-------------

  ┌─────────────────────────────────────────────────────────────────┐
  │  [check_wttr_available]  HttpSensor                             │
  │      Polls https://wttr.in/London?format=j1                     │
  │      Waits up to 5 min for a 200 OK response                    │
  └───────────────────┬─────────────────────────────────────────────┘
                      │
          ┌───────────┴──────────────┐
          ▼                          ▼
  [fetch_london_weather]      [fetch_forecast]
   HttpOperator                HttpOperator
   wttr.in/London              Open-Meteo London forecast
   → JSON via XCom             → JSON via XCom
          │                          │
          └───────────┬──────────────┘
                      ▼
            [parse_weather_data]
             PythonOperator
             Reads both XComs, logs summary
             Pushes alert_level to XCom
                      │
          ┌───────────┴──────────────┐
          ▼                          ▼
  [hot_weather_alert]         [normal_weather]
   BashOperator                BashOperator
   (if temp > 25°C)            (otherwise)
          │                          │
          └───────────┬──────────────┘
                      ▼
              [pipeline_complete]
               EmptyOperator

"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# DAG DEFAULT ARGUMENTS
#
# These are applied to every task unless overridden at the task level.
# Common fields:
#   owner        — who is responsible for this DAG (shown in UI)
#   retries      — how many times to retry a failed task automatically
#   retry_delay  — how long to wait between retries
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2026, 1, 1),
}

# DAG DEFINITION
#
# catchup=False  → do NOT run for every missed schedule since start_date.
#                  Without this, Airflow would try to "catch up" by running
#                  one DAG run per day going all the way back to 2026-01-01.
# schedule       → '@hourly' means once per hour; use None to disable auto-run
with DAG(
    dag_id='http_weather_dag',
    default_args=default_args,
    description='Demonstrates HttpSensor + HttpOperator with free weather APIs',
    schedule='@hourly',          # Run once per hour automatically
    catchup=False,
    max_active_runs=1,
    tags=['http', 'weather', 'learning', 'api'],
    doc_md=__doc__,              # The module docstring shows up in the Airflow UI
) as dag:

    # TASK 1 — HttpSensor: CHECK API IS AVAILABLE
    #
    # WHY USE A SENSOR?
    #   External services can be temporarily unavailable (maintenance windows,
    #   rate limits, network blips).  Rather than failing the whole pipeline,
    #   an HttpSensor will *wait* and keep retrying until the API responds.
    #
    # KEY PARAMETERS:
    #   http_conn_id   — The Airflow Connection to use.  'wttr_in' was
    #                    registered by airflow-init with host=wttr.in, schema=https.
    #                    Airflow builds the URL as: {schema}://{host}/{endpoint}
    #                    NOTE: In Airflow 3.0 this is `http_conn_id`, not `conn_id`.
    #   endpoint       — Path appended to the connection host.
    #                    wttr.in/London?format=j1 returns a JSON object.
    #   method         — HTTP verb (GET is the default).
    #   response_check — A callable that receives the Response object.
    #                    Return True = API is ready → proceed.
    #                    Return False = not ready yet → keep polling.
    #   poke_interval  — Seconds between each retry (30s here).
    #   timeout        — Total seconds to wait before failing (300s = 5 min).
    #   mode           — 'poke' occupies a worker slot while waiting.
    #                    Use 'reschedule' in production to free the slot.
    check_wttr_available = HttpSensor(
        task_id='check_wttr_available',
        http_conn_id='wttr_in',                    # Airflow 3.0: http_conn_id (not conn_id)
        endpoint='London?format=j1',               # Full path after the host
        method='GET',
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,                          # Check every 30 seconds
        timeout=300,                               # Give up after 5 minutes
        mode='poke',                               # Hold worker slot (fine for dev)
        doc_md="""
**HttpSensor** — Polls the wttr.in API for London weather.
Keeps retrying every 30 s until a 200 OK is returned.
This gates the rest of the pipeline: nothing runs until the API is live.
""",
    )

    # TASK GROUP: fetch_weather
    #
    # TaskGroups are a purely visual/organizational feature — they group tasks
    # in the Airflow UI "Graph" view so the DAG doesn't look cluttered.
    # They have no effect on execution order (that's still set by >>).
    with TaskGroup('fetch_weather', tooltip='Fetch data from both weather APIs') as fetch_group:

        # TASK 2a — HttpOperator: FETCH CURRENT WEATHER FROM wttr.in
        #
        # HOW HttpOperator WORKS:
        #   • Builds URL: {conn schema}://{conn host}/{endpoint}
        #   • Performs HTTP GET (or POST, PUT, etc.)
        #   • By default the raw response TEXT is pushed to XCom under the
        #     key 'return_value'.
        #   • response_filter lets you transform it before XCom storage —
        #     here we parse the JSON and pull out just the fields we need.
        #
        # WHAT wttr.in RETURNS (?format=j1):
        #   {
        #     "current_condition": [{
        #       "temp_C": "14",
        #       "humidity": "78",
        #       "weatherDesc": [{"value": "Partly cloudy"}],
        #       "windspeedKmph": "15"
        #     }],
        #     "nearest_area": [{"areaName": [{"value": "London"}]}],
        #     ...
        #   }
        fetch_london_weather = HttpOperator(
            task_id='fetch_london_weather',
            http_conn_id='wttr_in',                # Airflow 3.0: http_conn_id (not conn_id)
            endpoint='London?format=j1',
            method='GET',
            # response_filter receives a requests.Response object.
            # We parse JSON and return only the fields we care about.
            # This dict is what gets stored in XCom.
            response_filter=lambda response: {
                'city': 'London',
                'source': 'wttr.in',
                'temp_c': response.json()['current_condition'][0]['temp_C'],
                'humidity': response.json()['current_condition'][0]['humidity'],
                'description': response.json()['current_condition'][0]['weatherDesc'][0]['value'],
                'wind_kmph': response.json()['current_condition'][0]['windspeedKmph'],
                'fetched_at': datetime.utcnow().isoformat(),
            },
            log_response=True,   # Print raw response to task log (helpful for debugging)
            doc_md="""
**HttpOperator** — Calls wttr.in for London current conditions.
Uses `response_filter` to extract and reshape the JSON before XCom.
""",
        )

        # TASK 2b — HttpOperator: FETCH FORECAST FROM Open-Meteo
        #
        # Open-Meteo API format:
        #   GET https://api.open-meteo.com/v1/forecast
        #     ?latitude=51.5085
        #     &longitude=-0.1257
        #     &current=temperature_2m,relative_humidity_2m,wind_speed_10m
        #     &forecast_days=1
        #
        # The full URL becomes:
        #   https://api.open-meteo.com/v1/forecast?latitude=51.5...
        #
        # PARAMS NOTE:
        #   The `data` field on HttpOperator is passed as a query-string
        #   for GET requests.  Airflow url-encodes it automatically.
        fetch_forecast = HttpOperator(
            task_id='fetch_open_meteo_forecast',
            http_conn_id='open_meteo',             # Airflow 3.0: http_conn_id (not conn_id)
            endpoint='v1/forecast',
            method='GET',
            # Query string parameters for the Open-Meteo API
            data={
                'latitude': '51.5085',    # London latitude
                'longitude': '-0.1257',   # London longitude
                'current': 'temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code',
                'forecast_days': '1',
            },
            response_filter=lambda response: {
                'source': 'Open-Meteo',
                'latitude': response.json()['latitude'],
                'longitude': response.json()['longitude'],
                'current': response.json().get('current', {}),
                'fetched_at': datetime.utcnow().isoformat(),
            },
            log_response=True,
            doc_md="""
**HttpOperator** — Calls Open-Meteo for a London 1-day forecast.
Passes latitude/longitude as query-string `data` params.
""",
        )

    # TASK 3 — PythonOperator: PARSE AND SUMMARISE BOTH RESPONSES
    #
    # XCom (Cross-Communication) lets tasks share small pieces of data.
    # HttpOperator automatically pushes its return_value to XCom.
    # We retrieve it here with xcom_pull(task_ids=...).
    #
    # The task_ids for XCom must include the TaskGroup prefix:
    #   fetch_weather.fetch_london_weather
    #   fetch_weather.fetch_open_meteo_forecast
    def parse_and_summarise(**context):
        """
        Pull data from both HttpOperator tasks via XCom, log a summary,
        and push an alert_level so the branch task can decide what to do.

        XCom pull pattern:
            ti.xcom_pull(task_ids='<task_id>')
            → returns the 'return_value' pushed by that task

        For tasks inside a TaskGroup, include the group name:
            ti.xcom_pull(task_ids='fetch_weather.fetch_london_weather')
        """
        ti = context['task_instance']

        # Pull the dict returned by fetch_london_weather's response_filter
        wttr_data = ti.xcom_pull(task_ids='fetch_weather.fetch_london_weather')

        # Pull the dict returned by fetch_open_meteo_forecast's response_filter
        meteo_data = ti.xcom_pull(task_ids='fetch_weather.fetch_open_meteo_forecast')

        print("\n" + "=" * 60)
        print("HTTP WEATHER PIPELINE — COMBINED REPORT")
        print("=" * 60)

        # wttr.in
        if wttr_data:
            print(f"\n📡 Source: {wttr_data['source']}")
            print(f"   City        : {wttr_data['city']}")
            print(f"   Temperature : {wttr_data['temp_c']}°C")
            print(f"   Humidity    : {wttr_data['humidity']}%")
            print(f"   Conditions  : {wttr_data['description']}")
            print(f"   Wind        : {wttr_data['wind_kmph']} km/h")
        else:
            print("⚠ wttr.in data not available")
            wttr_data = {}

        # Open-Meteo
        if meteo_data:
            current = meteo_data.get('current', {})
            print(f"\n📡 Source: {meteo_data['source']}")
            print(f"   Coordinates : {meteo_data['latitude']}, {meteo_data['longitude']}")
            print(f"   Temperature : {current.get('temperature_2m', 'N/A')}°C")
            print(f"   Humidity    : {current.get('relative_humidity_2m', 'N/A')}%")
            print(f"   Wind        : {current.get('wind_speed_10m', 'N/A')} km/h")
        else:
            print("⚠ Open-Meteo data not available")
            meteo_data = {}

        print("=" * 60 + "\n")

        # Determine alert level
        # Use wttr.in temp if available, fall back to Open-Meteo
        temp_str = wttr_data.get('temp_c') or \
            str(meteo_data.get('current', {}).get('temperature_2m', '0'))
        try:
            temp = float(temp_str)
        except (ValueError, TypeError):
            temp = 0.0

        alert_level = 'hot' if temp > 25 else 'normal'
        print(f"ℹ Computed temperature: {temp}°C  →  alert_level = '{alert_level}'")

        # Push alert_level so the branch task can read it
        ti.xcom_push(key='alert_level', value=alert_level)
        ti.xcom_push(key='temperature', value=temp)

        return {'status': 'success', 'alert_level': alert_level, 'temperature': temp}

    parse_task = PythonOperator(
        task_id='parse_weather_data',
        python_callable=parse_and_summarise,
        doc_md="""
**PythonOperator** — Pulls XCom data from both HTTP tasks, prints a
combined weather summary, and decides the alert level based on temperature.
""",
    )

    # TASK 4 — BranchPythonOperator: CONDITIONAL ROUTING
    #
    # BranchPythonOperator evaluates a function and returns the task_id (or
    # a list of task_ids) of the next task(s) to run.
    # All other downstream tasks are SKIPPED automatically.
    #
    # This illustrates how to build data-driven conditional pipelines.
    def decide_alert_branch(**context):
        """
        Read the alert_level pushed by parse_weather_data and return the
        task_id that should run next.
        """
        ti = context['task_instance']
        alert_level = ti.xcom_pull(task_ids='parse_weather_data', key='alert_level')
        print(f"Branch decision: alert_level = '{alert_level}'")
        if alert_level == 'hot':
            return 'hot_weather_alert'
        return 'normal_weather'

    branch_task = BranchPythonOperator(
        task_id='check_temperature_threshold',
        python_callable=decide_alert_branch,
        doc_md="""
**BranchPythonOperator** — Routes execution based on alert_level XCom.
→ hot_weather_alert  (if temp > 25°C)
→ normal_weather     (otherwise)
""",
    )

    # TASKS 5a/5b — BashOperators: CONDITIONAL ALERT MESSAGES
    #
    # Bash operators demonstrate that you can run shell commands and also
    # access XCom values via Airflow's Jinja template syntax:
    #   {{ ti.xcom_pull(task_ids='...', key='...') }}
    hot_alert = BashOperator(
        task_id='hot_weather_alert',
        bash_command=(
            'echo "🌡 HOT WEATHER ALERT! '
            'Temperature is {{ ti.xcom_pull(task_ids=\"parse_weather_data\", key=\"temperature\") }}°C '
            '— above 25°C threshold."'
        ),
        doc_md="""
**BashOperator** — Fires only when temperature > 25°C.
Demonstrates Jinja templating to embed XCom values in bash commands.
""",
    )

    normal_weather = BashOperator(
        task_id='normal_weather',
        bash_command=(
            'echo "✅ Weather is normal. '
            'Temperature: {{ ti.xcom_pull(task_ids=\"parse_weather_data\", key=\"temperature\") }}°C"'
        ),
        doc_md="""
**BashOperator** — Fires when temperature is within normal range.
""",
    )

    # TASK 6 — EmptyOperator: TERMINAL MARKER
    #
    # EmptyOperator does nothing — it's used as a visual end-point to make
    # the DAG graph look clean.  trigger_rule=ONE_SUCCESS means it runs as
    # long as at least one upstream task succeeded (needed here because the
    # branch skips one of the two alert tasks).
    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete',
        trigger_rule=TriggerRule.ONE_SUCCESS,
        doc_md="""
**EmptyOperator** — Terminal marker.  `trigger_rule=ONE_SUCCESS` ensures
this runs even though one branch was skipped.
""",
    )

    # TASK DEPENDENCIES
    #
    # The >> operator sets the execution order.
    # Reading left-to-right: "A must complete before B starts".
    #
    # Full dependency graph:
    #
    #   check_wttr_available
    #          │
    #   ┌──────┴────────┐
    #   │ fetch_group   │
    #   │  fetch_london │
    #   │  fetch_meteo  │
    #   └──────┬────────┘
    #          │
    #   parse_weather_data
    #          │
    #   check_temperature_threshold
    #          │
    #   ┌──────┴──────────────┐
    #   hot_weather_alert   normal_weather
    #          │                  │
    #          └──────┬───────────┘
    #                 │
    #         pipeline_complete
    check_wttr_available >> fetch_group >> parse_task >> branch_task
    branch_task >> [hot_alert, normal_weather] >> pipeline_complete
