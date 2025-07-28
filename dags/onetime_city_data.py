from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import duckdb
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────
COORD_CSV_PATH = '/opt/airflow/data/city_long_lat.csv'
POP_CSV_PATH   = '/opt/airflow/data/city populations fmt.csv'
LOG_PATH       = '/opt/airflow/data/data_log'
DUCKDB_PATH    = '/opt/airflow/data/climate_dwh.db'
CITY_COORD_TABLE = 'land_city_coord'
CITY_POP_TABLE   = 'land_city_pop'

# ── Helper: Logging ───────────────────────────────────────────────────────
def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    logfile = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(logfile, 'a') as f:
        f.write(f"[{ts}] {message}\n")

# ── Task: Load City Coordinates ────────────────────────────────────────────
def load_city_coords(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, 'Starting load_city_coords')
    # Read CSV
    df = pd.read_csv(COORD_CSV_PATH)
    log_message(task_id, f"Read {len(df)} rows from {COORD_CSV_PATH}")
    # Add load timestamp
    ts = datetime.now(timezone.utc).isoformat()
    df['last_load_time'] = ts
    # Write to DuckDB
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.register('coords_df', df)
    con.execute(f"CREATE OR REPLACE TABLE {CITY_COORD_TABLE} AS SELECT * FROM coords_df")
    con.close()
    log_message(task_id, f"Created table {CITY_COORD_TABLE} with load_time={ts}")

# ── Task: Load City Populations ────────────────────────────────────────────
def load_city_populations(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, 'Starting load_city_populations')
    # Read CSV
    df = pd.read_csv(POP_CSV_PATH)
    log_message(task_id, f"Read {len(df)} rows from {POP_CSV_PATH}")
    # Add load timestamp
    ts = datetime.now(timezone.utc).isoformat()
    df['last_load_time'] = ts
    # Write to DuckDB
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.register('pop_df', df)
    con.execute(f"CREATE OR REPLACE TABLE {CITY_POP_TABLE} AS SELECT * FROM pop_df")
    con.close()
    log_message(task_id, f"Created table {CITY_POP_TABLE} with load_time={ts}")

# ── DAG Definition ──────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='load_city_data',
    default_args=default_args,
    description='Load city coordinates and populations into DuckDB',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['data_load', 'city'],
) as dag:
    load_coords = PythonOperator(
        task_id='load_city_coords',
        python_callable=load_city_coords
    )

    load_pop = PythonOperator(
        task_id='load_city_populations',
        python_callable=load_city_populations
    )

    load_coords >> load_pop
