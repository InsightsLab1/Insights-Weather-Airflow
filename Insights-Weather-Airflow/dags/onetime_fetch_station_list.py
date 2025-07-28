from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import pandas as pd
import duckdb
from meteostat import Stations

# ── Paths 
LOG_PATH          = '/opt/airflow/data/data_log'
STATION_LIST_PATH = '/opt/airflow/data/meteostat_stations.json'
DUCKDB_PATH       = '/opt/airflow/data/climate_dwh.db'
DUCKDB_TABLE      = 'land_obs_stations'

# ── Helpers ─────────────────────────────────────────────────────────────
def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    log_file = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")

# ── Task Functions ──────────────────────────────────────────────────────
def fetch_and_save_all_stations(**kwargs):
    """
    1) Fetch every station globally via Stations().fetch()
    2) Add a snapshot_time watermark and save all stations to JSON
    """
    os.makedirs(os.path.dirname(STATION_LIST_PATH), exist_ok=True)

    stations = Stations()
    df_all = stations.fetch().reset_index()
    df_all['snapshot_time'] = datetime.now(timezone.utc).isoformat()

    df_all.to_json(
        STATION_LIST_PATH,
        orient='records',
        date_format='iso',
        force_ascii=False,
        indent=2
    )

    msg = f"Saved {len(df_all)} global station records (snapshot at {df_all['snapshot_time'].iloc[0]}) to {STATION_LIST_PATH}"
    log_message(kwargs['task'].task_id, msg)


def load_json_to_duckdb(**kwargs):
    """
    1) Read the previously written JSON into a pandas DataFrame
    2) Append to DuckDB table if exists, or create it if not
    """
    df = pd.read_json(STATION_LIST_PATH, orient='records')

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.register('df', df)
    
    # Check if the target table exists
    table_exists = con.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema='main' AND table_name='{DUCKDB_TABLE}'
    """).fetchone()[0] > 0

    if table_exists:
        con.execute(f"INSERT INTO {DUCKDB_TABLE} SELECT * FROM df")
        action = 'Appended'
    else:
        con.execute(f"CREATE TABLE {DUCKDB_TABLE} AS SELECT * FROM df")
        action = 'Created'

    con.close()

    msg = f"{action} table '{DUCKDB_TABLE}' with {len(df)} rows in {DUCKDB_PATH}"  
    log_message(kwargs['task'].task_id, msg)

# ── DAG Definition ──────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='meteostat_us_stations_to_duckdb',
    default_args=default_args,
    description='Fetch all station metadata from Meteostat → JSON → DuckDB',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['meteostat', 'stations', 'duckdb'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_save_all_stations',
        python_callable=fetch_and_save_all_stations
    )

    load_task = PythonOperator(
        task_id='load_json_to_duckdb',
        python_callable=load_json_to_duckdb
    )

    fetch_task >> load_task
