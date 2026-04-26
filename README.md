# airflow-lab

A hands-on learning project built with **Apache Airflow 3.0.6**.  
It demonstrates real-world ETL patterns, HTTP API integration, branching pipelines,  
and Airflow core concepts explained from first principles.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Why the Login Broke](#why-the-login-broke)
3. [Project Structure](#project-structure)
4. [DAGs in This Project](#dags-in-this-project)
5. [Core Airflow Concepts](#core-airflow-concepts)
6. [HTTP Weather DAG Walkthrough](#http-weather-dag-walkthrough)
7. [Weather Pipeline ETL Walkthrough](#weather-pipeline-etl-walkthrough)
8. [Free APIs](#free-apis)
9. [Database Access](#database-access)
10. [Configuration](#configuration)
11. [Troubleshooting](#troubleshooting)
12. [Extending the Project](#extending-the-project)

---

## Quick Start

### Prerequisites

| Requirement | Minimum |
|-------------|---------|
| Docker Desktop | Latest stable |
| Docker Compose | v2+ (ships with Docker Desktop) |
| RAM | 4 GB available |
| Free ports | 8080 (Airflow UI), 5432 (PostgreSQL) |

### Start Everything

```bash
# From the project root:
docker-compose down -v        # Remove old containers AND volumes (fresh start)
docker-compose up -d          # Start all services in the background
```

### Wait for Startup (~60 seconds)

The startup sequence is:
1. **postgres** starts and becomes healthy
2. **airflow-init** migrates the DB and creates the `admin` user
3. **airflow-webserver** (standalone) starts *after* init completes

```bash
# Watch live status:
docker-compose ps

# Expected final state:
#  NAME                          STATUS
#  airflow-airflow-init-1        Exit 0        ← init finished successfully
#  airflow-airflow-webserver-1   Up (healthy)
#  airflow-postgres-1            Up (healthy)
```

### Login to Airflow UI

Open: **http://localhost:8080**

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `admin123` |


---

## Why the Login Broke

### The Problem

You set `AIRFLOW__STANDALONE__ADMIN_PASSWORD: 'admin123'` in `docker-compose.yml` and
expected `admin / admin123` to always work.  It works on the **very first run** but
breaks on restarts.  Here is exactly why:

**Airflow 3.0 changed authentication architecture completely.**  
Airflow 2.x stored passwords in the PostgreSQL `ab_user` table (Flask-AppBuilder).  
Airflow 3.0 uses **SimpleAuthManager** — a **file-based** auth system.  
Passwords live in: `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`

**What happens on first run:**
```
docker-compose up -d
  → standalone starts, no password file found
  → reads AIRFLOW__STANDALONE__ADMIN_PASSWORD = admin123
  → writes simple_auth_manager_passwords.json.generated  ← file now exists
  → ✅ admin / admin123 works
```

**What goes wrong with a naïve approach:**
```
AIRFLOW__STANDALONE__ADMIN_PASSWORD   ← THIS ENV VAR DOES NOT EXIST IN AIRFLOW 3.0
docker-compose restart
  → standalone starts, password file EXISTS from previous run
  → standalone reads the file, new random password inside
  → ❌ admin / admin123 does NOT work
```

**Airflow 3.0 SimpleAuthManager logic in `find_user_info()`:**
```
if password_file EXISTS  → use existing passwords (skips random generation)
if password_file MISSING → call am.init() → generates RANDOM password, writes to file
```

### The Fix: Pre-Write the Password File

The **real** fix in Airflow 3.0 is to **write** the password file with our known password
**before** starting standalone. The webserver command now does this:

```bash
# Write the SimpleAuthManager password file with admin:admin123
echo '{"admin": "admin123"}' > /opt/airflow/simple_auth_manager_passwords.json.generated
exec airflow standalone   # ← standalone finds the file, uses admin123, skips random generation
```

File format: plain JSON — `{"username": "plaintext-password"}`.

When standalone starts and calls `SimpleAuthManager.init()`, it finds the file already exists
and skips generating a new random password. The user logs in with `admin / admin123` ✅.

> **Note**: `AIRFLOW__STANDALONE__ADMIN_PASSWORD` was an **Airflow 2.x** env var.  
> It was **removed in Airflow 3.0** and has zero effect. Remove it from any configs.

### Why `airflow users create` Does NOT Work in Airflow 3.0

The old fix (delete + recreate user via CLI) wrote to the **PostgreSQL** `ab_user` table.  
SimpleAuthManager ignores that table entirely.  Running `airflow users create` in  
Airflow 3.0 creates a user in a place that auth never checks.  That is why our  
previous init-container approach did not fix the login — it was writing to the wrong place.

---

## Project Structure

```
airflow/
├── dags/
│   ├── hello_world.py           # Beginner DAG: PythonOperator, BashOperator
│   ├── weather_pipeline_dag.py  # Full ETL: Extract → Transform → Load → Report
│   └── http_weather_dag.py      # HTTP provider: HttpSensor + HttpOperator
├── plugins/
│   ├── operators/               # Custom operators (extend as needed)
│   └── utils/                   # Shared utility functions
├── config/                      # Airflow config overrides
├── data/                        # Output files (CSV exports, downloads)
├── logs/                        # Airflow task execution logs
├── tests/                       # Unit and integration tests
├── docs/                        # Additional documentation
├── docker-compose.yml           # Three services: postgres, airflow-init, airflow-webserver
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## DAGs in This Project

| DAG ID | Schedule | Description |
|--------|----------|-------------|
| `hello_world` | Daily | Intro: PythonOperator, BashOperator, EmptyOperator, XCom |
| `weather_pipeline_dag` | 6 AM UTC | Full ETL: mock data → Postgres + CSV + report |
| `http_weather_dag` | Hourly | **HttpSensor + HttpOperator** with free live weather APIs |

---

## Core Airflow Concepts

### What is a DAG?

A **DAG** (Directed Acyclic Graph) is how Airflow represents a pipeline.

- **Directed** — tasks have a defined execution order
- **Acyclic** — no circular dependencies
- **Graph** — visualised as a flowchart in the Airflow UI

```python
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id='my_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,               # Don't back-fill missed runs
) as dag:
    ...  # define tasks here
```

### Operators

| Operator | When to Use |
|----------|-------------|
| `PythonOperator` | Run any Python function |
| `BashOperator` | Run shell commands |
| `EmptyOperator` | Start/end markers, visual anchors |
| `BranchPythonOperator` | Conditional routing |
| `FileSensor` | Wait until a file appears on disk |
| **`HttpSensor`** | Wait until an HTTP endpoint responds |
| **`HttpOperator`** | Make HTTP GET/POST requests |

### Connections

A **Connection** stores credentials and endpoint info so DAG code contains no hardcoded URLs or passwords.

```
Stored in Airflow (DB or UI):
  conn_id   = wttr_in
  conn_type = http
  host      = wttr.in
  schema    = https

In your DAG:
  HttpOperator(http_conn_id='wttr_in', endpoint='London?format=j1')

Airflow builds URL:  https://wttr.in/London?format=j1
```

Manage connections:
- **UI**: Admin → Connections → + button
- **CLI**: `airflow connections add <id> --conn-type http --conn-host ...`
- **Automated**: registered by `airflow-init` in `docker-compose.yml`

### XCom — Passing Data Between Tasks

**XCom** (Cross-Communication) lets tasks share small values through the Airflow database.

```python
# PUSH — return a value (auto-pushed as 'return_value')
def task_a(**context):
    return {"city": "London", "temp": 15}

# PUSH — manual with a custom key
def task_a(**context):
    context['task_instance'].xcom_push(key='alert_level', value='hot')

# PULL
def task_b(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='task_a')                     # return_value
    level = ti.xcom_pull(task_ids='task_a', key='alert_level') # named key
```

**In Bash (Jinja templates):**
```bash
echo "Temp: {{ ti.xcom_pull(task_ids='parse', key='temperature') }}°C"
```

> XCom is for **small** values only. Don't push DataFrames or binary files.

### HttpSensor vs HttpOperator

| | `HttpSensor` | `HttpOperator` |
|---|---|---|
| **Purpose** | *Wait* until API is ready | *Fetch* data from API |
| **Runs until** | `response_check` returns `True` | First successful response |
| **Output** | Boolean (success/fail) | Response content → XCom |
| **Use case** | Gate the pipeline on API availability | Retrieve data for processing |

```python
# HttpSensor — "Is the API up yet?"
HttpSensor(
    task_id='check_api',
    http_conn_id='wttr_in',
    endpoint='London?format=j1',
    response_check=lambda r: r.status_code == 200,
    poke_interval=30,    # Check every 30 seconds
    timeout=300,         # Give up after 5 minutes
    mode='poke',
)

# HttpOperator — "Give me the data"
HttpOperator(
    task_id='fetch_weather',
    http_conn_id='wttr_in',
    endpoint='London?format=j1',
    method='GET',
    response_filter=lambda r: r.json()['current_condition'][0],
    log_response=True,
)
```

### TaskGroups

TaskGroups visually group tasks in the UI Graph view.  No effect on execution order.

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup('fetch_weather', tooltip='Calls two APIs') as fetch_group:
    task_a = HttpOperator(task_id='fetch_wttr', ...)
    task_b = HttpOperator(task_id='fetch_meteo', ...)

# XCom must include the group prefix:
#   ti.xcom_pull(task_ids='fetch_weather.fetch_wttr')
```

### BranchPythonOperator

Returns a `task_id` (or list) — only that task runs, all others in the fork are **Skipped**.

```python
def decide(**context):
    temp = context['ti'].xcom_pull(task_ids='parse', key='temperature')
    return 'hot_alert' if float(temp) > 25 else 'normal_path'

branch = BranchPythonOperator(task_id='route', python_callable=decide)

# Terminal task needs ONE_SUCCESS because one branch is always skipped
end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ONE_SUCCESS)
```

---

## HTTP Weather DAG Walkthrough

**File**: `dags/http_weather_dag.py`

Uses only free APIs — no signup or API key needed.

### Pipeline Flow

```
[check_wttr_available]          HttpSensor
  Polls wttr.in every 30 s
  Waits up to 5 min for 200 OK
        │
        ▼
┌──────────────────────────────┐
│         fetch_weather        │  TaskGroup
│  [fetch_london_weather]      │  HttpOperator → wttr.in JSON
│  [fetch_open_meteo_forecast] │  HttpOperator → Open-Meteo forecast
└──────────────┬───────────────┘
               │
               ▼
      [parse_weather_data]       PythonOperator
        Reads both XComs, logs summary
        Pushes 'temperature' + 'alert_level' to XCom
               │
               ▼
  [check_temperature_threshold]  BranchPythonOperator
               │
     ┌─────────┴──────────┐
     ▼                    ▼
[hot_weather_alert]  [normal_weather]   BashOperators (use Jinja XCom)
     │                    │
     └─────────┬──────────┘
               ▼
      [pipeline_complete]   EmptyOperator (trigger_rule=ONE_SUCCESS)
```

### Key Concepts Per Task

| Task | Concept Demonstrated |
|------|---------------------|
| `check_wttr_available` | HttpSensor polling with poke_interval + timeout |
| `fetch_london_weather` | HttpOperator + `response_filter` lambda |
| `fetch_open_meteo_forecast` | HttpOperator + `data` query-string params |
| `parse_weather_data` | XCom pull with TaskGroup prefix in task_ids |
| `check_temperature_threshold` | BranchPythonOperator conditional routing |
| `hot_weather_alert` / `normal_weather` | Jinja template `{{ ti.xcom_pull(...) }}` in bash |
| `pipeline_complete` | TriggerRule.ONE_SUCCESS to handle skipped branch |

### How to Run It

1. Open Airflow UI → **http://localhost:8080**
2. Find **http_weather_dag**
3. Toggle it **ON** (blue switch on the left)
4. Click **▶ Trigger DAG** for an immediate run
5. Click the DAG name → **Graph** tab to watch tasks execute

---

## Weather Pipeline ETL Walkthrough

**File**: `dags/weather_pipeline_dag.py`

Classic Extract → Transform → Load pattern with simulated weather data.

```
[extract_weather_data]     Mock data for London, New York, Tokyo
          │
          ▼
[transform_weather_data]   Validate + enrich with pandas
          │
    ┌─────┴──────┐
    ▼            ▼          ← Parallel execution
[load_to_postgres] [load_to_csv]
    │            │
    └─────┬──────┘
          ▼
  [generate_report]   Statistics + summary
```

The `>>` operator with a list runs tasks in parallel:
```python
transform_task >> [load_postgres_task, load_csv_task] >> report_task
```

---

## Free APIs

### wttr.in

- **URL**: `https://wttr.in/:city?format=j1`
- **Auth**: None required
- **Docs**: https://github.com/chubin/wttr.in
- **Example**: `curl "https://wttr.in/London?format=j1"`

```json
{
  "current_condition": [{
    "temp_C": "14", "humidity": "78",
    "weatherDesc": [{"value": "Partly cloudy"}],
    "windspeedKmph": "15"
  }]
}
```

### Open-Meteo

- **URL**: `https://api.open-meteo.com/v1/forecast`
- **Auth**: None required
- **Docs**: https://open-meteo.com/en/docs
- **Example**: `curl "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.12&current=temperature_2m"`

---

## Database Access

```bash
# Connect
docker-compose exec postgres psql -U airflow -d airflow

# Useful commands
\dt                    # list tables
\d weather_data        # describe table
\q                     # quit
```

```sql
-- All weather records
SELECT * FROM weather_data ORDER BY created_at DESC LIMIT 10;

-- Avg temp per city
SELECT city, ROUND(AVG(temp)::numeric, 1) AS avg_temp FROM weather_data GROUP BY city;

-- Extreme events
SELECT city, temp, description FROM weather_data WHERE is_extreme = true;

-- Recent DAG runs
SELECT dag_id, state, start_date FROM dag_run ORDER BY start_date DESC LIMIT 10;

-- Admin users (Airflow 2.x table — NOTE: not used by Airflow 3.0 auth)
SELECT username, email FROM ab_user;
```

---

## Configuration

### Schedule Syntax

| Preset | Meaning |
|--------|---------|
| `@once` | Run once only |
| `@hourly` | Every hour |
| `@daily` | Every day at midnight UTC |
| `None` | No automatic runs |
| `'0 6 * * *'` | Daily at 6 AM UTC |
| `'0 */6 * * *'` | Every 6 hours |

### Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` | Define users as `username:role` (e.g. `admin:Admin`) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | PostgreSQL connection string |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` = single-node |
| `AIRFLOW__CORE__FERNET_KEY` | Encryption key for stored credentials |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` = hide built-in example DAGs |

> ⚠️ **`AIRFLOW__STANDALONE__ADMIN_PASSWORD` does NOT exist in Airflow 3.0.** It was removed.  
> Passwords are written directly to the `simple_auth_manager_passwords.json.generated` file (see webserver command).

---

## Troubleshooting

### "Wrong password / invalid credentials"

This is an Airflow 3.0 SimpleAuthManager issue.  The **definitive fix**:

```bash
docker-compose down -v           # -v removes ALL volumes (fresh DB)
docker-compose up -d             # Full clean start — always produces admin/admin123
```

Without wiping volumes (preserves DAG run history):
```bash
docker-compose restart airflow-webserver
# The webserver command always writes {"admin": "admin123"} to the password file
# before starting standalone — so admin123 is always the password after any restart
```

### "Connection refused to localhost:8080"

```bash
docker-compose ps                              # Is the webserver running?
docker-compose logs airflow-webserver | tail -50   # Any startup errors?
docker-compose restart airflow-webserver
# Wait ~60 seconds for all sub-processes to start
```

### "DAG not showing in UI"

```bash
# Check for syntax errors
docker-compose exec airflow-webserver python -m py_compile dags/http_weather_dag.py

# Check scheduler picked it up
docker-compose logs airflow-webserver | grep -i "http_weather_dag"

# Force reserialize
docker-compose exec airflow-webserver airflow dags reserialize
```

### "HttpSensor timing out"

```bash
# Test API reachability from inside the container
docker-compose exec airflow-webserver curl -s "https://wttr.in/London?format=j1" | head -c 200
docker-compose exec airflow-webserver curl -s "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.12&current=temperature_2m" | head -c 200
```

### "Tasks failing — no error message"

1. Click the failed task in the UI
2. Click **Logs** tab
3. Look for Python tracebacks or `ERROR` lines

---

## Extending the Project

### Add a New Task

```python
def my_task(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='previous_task')
    return process(data)   # auto-pushed to XCom

new_task = PythonOperator(task_id='my_task', python_callable=my_task)
existing_task >> new_task >> downstream_task
```

### Add an HTTP API Call

```python
# 1. Register connection (UI or airflow-init):
#    conn_id=my_api, host=api.example.com, schema=https

# 2. Use in DAG:
HttpOperator(
    task_id='call_api',
    conn_id='my_api',
    endpoint='v1/data',
    method='GET',
    response_filter=lambda r: r.json(),
)
```

### Use Variables for Secrets

```bash
docker-compose exec airflow-webserver airflow variables set MY_API_KEY "abc123"
# or UI: Admin → Variables → +
```

```python
from airflow.models import Variable
api_key = Variable.get("MY_API_KEY", default="demo")
```

---

## Resources

| Resource | URL |
|----------|-----|
| Apache Airflow 3.0 Docs | https://airflow.apache.org/docs/ |
| HTTP Provider Docs | https://airflow.apache.org/docs/apache-airflow-providers-http/ |
| SimpleAuthManager Docs | https://airflow.apache.org/docs/apache-airflow/stable/security/auth-manager/simple-auth-manager.html |
| wttr.in API | https://github.com/chubin/wttr.in |
| Open-Meteo API | https://open-meteo.com/en/docs |
| Cron Helper | https://crontab.guru |

---

## License

Educational project — MIT License

**Open http://localhost:8080 and trigger `http_weather_dag` to get started.**