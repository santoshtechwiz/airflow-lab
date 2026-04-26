"""
Hello World DAG — Beginner Introduction
=========================================

PURPOSE
-------
The simplest possible Airflow DAG.  Start here before reading any other DAG
in this project.  It covers the four fundamental building blocks you will use
in every real pipeline:

  1. DAG            — the container that defines the pipeline
  2. Operators      — the units of work (tasks)
  3. Task dependencies — the arrows between tasks (>> operator)
  4. XCom           — the mechanism for passing data between tasks

PIPELINE FLOW
-------------

  [start]
     │
  [greet]  ──────────────────────────────────────────────────────┐
  PythonOperator                                                  │
  Prints "Hello, World!" and pushes                               │
  the string "Greeting sent" to XCom (automatically, via return) │
     │                                                            │
     ├──────────────────────┐                                     │
     ▼                      ▼                                     │
  [bash_echo]          [process_greeting]                         │
  BashOperator          PythonOperator                            │
  Runs a shell           Pulls the XCom value from               │
  command and            greet_task and logs it                   │
  prints the date        (demonstrates xcom_pull)                 │
     │                      │                                     │
     └──────────┬───────────┘                                     │
                ▼                                                  │
             [end]  ◄────────────────────────────────────────────┘
             EmptyOperator
             Terminal marker — no logic, just a clean endpoint

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# DEFAULT ARGUMENTS
#
# A dict of parameters applied to every task in this DAG by default.
# You can override any of these at the individual task level.
#
# Common fields:
#   owner          → label shown in the Airflow UI (who manages this DAG)
#   retries        → automatically retry a failed task this many times
#   retry_delay    → wait this long between retries
#   start_date     → the earliest date a DAG run can be scheduled from
#   email_on_*     → disable email alerts (we have no SMTP configured)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG DEFINITION
#
# Using the `with DAG(...) as dag:` context manager is the modern way to
# define a DAG.  Any operator created inside the `with` block is
# automatically registered to this DAG — no need to pass `dag=dag` to
# every operator.
#
# Key arguments:
#   dag_id       → unique identifier shown in the Airflow UI
#   schedule     → cron string, timedelta, or preset like '@daily'.
#                  timedelta(days=1) = run once per day.
#                  None = only run when triggered manually.
#   catchup      → False = only run for TODAY, not for every missed day
#                  going back to start_date
#   tags         → labels for filtering in the Airflow UI
with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='Beginner intro: PythonOperator, BashOperator, XCom, dependencies',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['beginner', 'intro', 'learning'],
    doc_md=__doc__,           # module docstring appears in the Airflow UI "Docs" tab
) as dag:

    # TASK 1 — EmptyOperator: START MARKER
    #
    # EmptyOperator does absolutely nothing — it has no execute() logic.
    # Used as a visual anchor: a single "start" node that all downstream
    # tasks wait for before beginning.
    start = EmptyOperator(task_id='start')

    # TASK 2 — PythonOperator: GREET
    #
    # PythonOperator calls any Python callable as a task.
    #
    # The function receives **context (a dict of Airflow runtime info:
    # task_instance, execution_date, dag_run, etc.) when the task executes.
    #
    # The RETURN VALUE is automatically pushed to XCom under the key
    # 'return_value'.  Downstream tasks can read it with:
    #   ti.xcom_pull(task_ids='greet_task')
    def greet(**context):
        """Print a greeting and return a string that goes to XCom."""
        message = "Hello, World from Airflow!"
        print(message)
        # Returning a value automatically pushes it to XCom
        return "Greeting sent"

    greet_task = PythonOperator(
        task_id='greet_task',
        python_callable=greet,
    )

    # TASK 3 — BashOperator: BASH ECHO
    #
    # BashOperator runs a shell command in a temporary bash subprocess.
    # Good for: running scripts, calling CLI tools, simple file operations.
    #
    # The `bash_command` string is processed as a Jinja template, so you
    # can embed {{ execution_date }}, {{ ds }}, {{ ti.xcom_pull(...) }}, etc.
    bash_task = BashOperator(
        task_id='bash_echo',
        bash_command='echo "Hello from Bash! Execution date: {{ ds }}" && date',
        doc_md="""
**BashOperator** — Runs a shell command.
`{{ ds }}` is a Jinja template that injects the execution date (YYYY-MM-DD).
""",
    )

    # TASK 4 — PythonOperator: PROCESS GREETING (XCom pull demo)
    #
    # This task shows how to READ data that another task wrote to XCom.
    #
    # xcom_pull(task_ids='greet_task') returns the 'return_value' that the
    # greet() function returned — which is the string "Greeting sent".
    def process_greeting(**context):
        """
        Pull the XCom value from greet_task and log it.

        ti = task_instance — the runtime object for this task.
        xcom_pull reads a value another task pushed to XCom.
        """
        ti = context['ti']
        greeting = ti.xcom_pull(task_ids='greet_task')   # reads 'return_value'
        print(f"XCom received from greet_task: '{greeting}'")
        return f"Processed: {greeting}"

    process_task = PythonOperator(
        task_id='process_greeting',
        python_callable=process_greeting,
        doc_md="""
**PythonOperator** — Reads XCom from `greet_task`.
Demonstrates `ti.xcom_pull(task_ids='greet_task')`.
""",
    )

    # TASK 5 — EmptyOperator: END MARKER
    end = EmptyOperator(task_id='end')

    # TASK DEPENDENCIES
    #
    # The >> operator sets the execution order.
    # task_a >> task_b  means: run task_b only after task_a succeeds.
    # task >> [b, c]    means: run b and c IN PARALLEL after task.
    #
    # Reading this graph:
    #   start → greet_task → [bash_echo, process_greeting] → end
    #
    # bash_echo and process_greeting run at the same time (parallel),
    # both waiting for greet_task first.
    start >> greet_task >> [bash_task, process_task] >> end
