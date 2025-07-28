from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import duckdb
import pandas as pd
import warnings
import traceback
import time
from meteostat import Hourly

# ── Configuration ───────────────────────────────────────────────────
DEFAULT_COUNTRIES  = ['US']
DEFAULT_STATES     = []
DEFAULT_BATCH_SIZE = 1

# ── Paths and Tables ─────────────────────────────────────────
LOG_PATH       = '/opt/airflow/data/data_log'
DUCKDB_PATH    = '/opt/airflow/data/climate_dwh.db'
STATION_TABLE  = 'land_obs_stations'
HOURLY_TABLE   = 'land_obs_hourly'
LOG_TABLE      = 'land_obs_load_log'

# ── Helper: Logging ────────────────────────────────────

def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    log_file = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(log_file, 'a') as f:
        f.write(f"[{ts}] {message}\n")

# ── Core Task ────────────────────────────────────

def initial_load_hourly(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, "Starting initial_load_hourly")

    conf = getattr(kwargs.get('dag_run'), 'conf', {}) or {}
    countries  = conf.get('countries') or DEFAULT_COUNTRIES
    states     = conf.get('states')    or DEFAULT_STATES
    batch_size = int(conf.get('batch_size', DEFAULT_BATCH_SIZE))
    log_message(task_id, f"Config - countries: {countries}, states: {states}, batch_size: {batch_size}")

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    query = f"SELECT id, country, region, hourly_start AS start_str, hourly_end AS end_str FROM {STATION_TABLE}"
    stations_df = con.execute(query).df()
    log_message(task_id, f"Loaded {len(stations_df)} stations with hourly bounds from {STATION_TABLE}")

    stations_df = stations_df.dropna(subset=['start_str', 'end_str'])
    if countries:
        stations_df = stations_df[stations_df['country'].isin(countries)]
    if states:
        stations_df = stations_df[stations_df['region'].isin(states)]
    log_message(task_id, f"{len(stations_df)} stations after filtering")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            station_id VARCHAR,
            last_fetched TIMESTAMP,
            table_name VARCHAR
        )
    """)
    loaded = set(con.execute(f"SELECT station_id, table_name FROM {LOG_TABLE}").fetchall())
    log_message(task_id, f"Existing load-log entries: {len(loaded)}")

    remaining_df = stations_df[~stations_df['id'].isin([sid for sid, table in loaded if table == HOURLY_TABLE])]
    log_message(task_id, f"{len(remaining_df)} total stations still to load into {HOURLY_TABLE}")

    candidates = []
    for _, row in stations_df.iterrows():
        key = (row['id'], HOURLY_TABLE)
        if key not in loaded:
            candidates.append(row)
            if len(candidates) >= batch_size:
                break
    selected_ids = [row['id'] for row in candidates]
    log_message(task_id, f"Selected stations for initial load: {selected_ids}")

    if not candidates:
        log_message(task_id, "No new stations to process for initial hourly load.")
        con.close()
        return

    dfs = []
    for row in candidates:
        sid       = row['id']
        start_str = row['start_str']
        end_str   = row['end_str']
        log_message(task_id, f"Fetching hourly for {sid}: {start_str} → {end_str}")
        try:
            start = datetime.fromisoformat(start_str)
            end   = datetime.fromisoformat(end_str)
            with warnings.catch_warnings(record=True) as warns:
                warnings.simplefilter('always')
                warnings.filterwarnings("ignore", category=FutureWarning, message=".*keepdims.*")
                log_message(task_id, f"About to create Hourly object for {sid}")
                hourly_obj = Hourly(sid, start, end)
                log_message(task_id, f"Created Hourly object for {sid}, about to call fetch()")
                df = hourly_obj.fetch()
                log_message(task_id, f"Completed fetch() for {sid}, rows: {len(df) if df is not None else 'None'}")

            for w in warns:
                log_message(task_id, f"Warning for {sid}: {w.message}")

            if df is not None and hasattr(df, 'empty') and not df.empty:
                df = df.reset_index()
                df['station_id'] = sid
                df['load_time']  = datetime.now(timezone.utc).isoformat()
                dfs.append(df)
            else:
                log_message(task_id, f"No data for {sid}; marking as fetched.")
        except Exception as e:
            tb = traceback.format_exc()
            log_message(task_id, f"Error fetching {sid}: {e}\n{tb}")
        time.sleep(0.5)

    ts_now = datetime.now(timezone.utc)
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        con.register('df_all', combined)
        try:
            con.begin()
            exists = con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='{HOURLY_TABLE}'"
            ).fetchone()[0] > 0

            if not exists:
                con.execute(f"CREATE TABLE {HOURLY_TABLE} AS SELECT * FROM df_all")
            else:
                con.execute(f"INSERT INTO {HOURLY_TABLE} SELECT * FROM df_all")

            for sid in selected_ids:
                con.execute(f"DELETE FROM {LOG_TABLE} WHERE station_id = '{sid}' AND table_name = '{HOURLY_TABLE}'")
                con.execute(f"INSERT INTO {LOG_TABLE} VALUES ('{sid}', TIMESTAMP '{ts_now}', '{HOURLY_TABLE}')")

            con.commit()
            log_message(task_id, f"Inserted {len(combined)} rows and updated log for stations {selected_ids}")

        except Exception as e:
            con.rollback()
            tb = traceback.format_exc()
            log_message(task_id, f"ROLLBACK: Error during insert/log update for stations {selected_ids}: {e}\n{tb}")
    else:
        try:
            con.begin()
            for sid in selected_ids:
                con.execute(f"DELETE FROM {LOG_TABLE} WHERE station_id = '{sid}' AND table_name = '{HOURLY_TABLE}'")
                con.execute(f"INSERT INTO {LOG_TABLE} VALUES ('{sid}', TIMESTAMP '{ts_now}', '{HOURLY_TABLE}')")
            con.commit()
            log_message(task_id, f"No data returned, but updated load-log for stations {selected_ids}")
        except Exception as e:
            con.rollback()
            tb = traceback.format_exc()
            log_message(task_id, f"ROLLBACK: Error updating log for empty fetch on {selected_ids}: {e}\n{tb}")

    con.close()

# ── DAG Definition ────────────────────────────────────

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
    'depends_on_past': False,
    'email_on_failure': False,
}

with DAG(
    dag_id='meteostat_hourly_obs_load',
    default_args=default_args,
    description='Initial batch‐load of Meteostat hourly data into DuckDB',
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(seconds=30),
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'meteostat'],
) as dag:
    initial_load = PythonOperator(
        task_id='initial_load_hourly',
        python_callable=initial_load_hourly
    )
    initial_load
