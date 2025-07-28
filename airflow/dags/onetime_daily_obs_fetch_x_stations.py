from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import duckdb
import pandas as pd
import warnings
import traceback
from meteostat import Daily

# ── Configuration ─────────────────────────────────────────────────────────
DEFAULT_COUNTRIES  = ['US']  # Empty list means all countries
DEFAULT_STATES     = []      # Empty list means all states/provinces
DEFAULT_BATCH_SIZE = 1       # Number of stations per batch run

# ── Paths and Tables ──────────────────────────────────────────────────────
LOG_PATH      = '/opt/airflow/data/data_log'
DUCKDB_PATH    = '/opt/airflow/data/climate_dwh.db'
STATION_TABLE = 'land_obs_stations'  # metadata table name (includes daily_start/daily_end)
DATA_TABLE    = 'land_obs_daily'     # target data table for daily observations
LOG_TABLE     = 'land_obs_load_log'  # tracking table for loads

# ── Helper: Logging ───────────────────────────────────────────────────────
def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    log_file = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(log_file, 'a') as f:
        f.write(f"[{ts}] {message}\n")

# ── Core Task ────────────────────────────────────────────────────────────
def fetch_and_append_stations(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, "Starting fetch_and_append_stations")

    # Read DAG run configuration
    conf = getattr(kwargs.get('dag_run'), 'conf', {}) or {}
    countries  = conf.get('countries') or DEFAULT_COUNTRIES
    states     = conf.get('states')    or DEFAULT_STATES
    batch_size = int(conf.get('batch_size', DEFAULT_BATCH_SIZE))
    log_message(task_id, f"Config - countries: {countries}, states: {states}, batch_size: {batch_size}")

    # Connect to DuckDB
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    # Load station metadata: id, daily_start, daily_end, country, region
    query = (
        f"SELECT id, country, region, daily_start AS start_str, daily_end AS end_str "
        f"FROM {STATION_TABLE}"
    )
    stations_df = con.execute(query).df()
    log_message(task_id, f"Loaded {len(stations_df)} stations with date bounds from {STATION_TABLE}")

    # Filter out stations missing bounds
    stations_df = stations_df.dropna(subset=['start_str', 'end_str'])
    # Filter by country/state
    if countries:
        stations_df = stations_df[stations_df['country'].isin(countries)]
    if states:
        stations_df = stations_df[stations_df['region'].isin(states)]
    log_message(task_id, f"{len(stations_df)} stations after filtering")

    # Ensure load-log table exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            station_id VARCHAR,
            last_fetched TIMESTAMP,
            table_name VARCHAR
        )
    """)
    loaded = set(con.execute(f"SELECT station_id, table_name FROM {LOG_TABLE}").fetchall())
    log_message(task_id, f"Existing load-log entries: {len(loaded)}")

    # Select next batch of stations
    candidates = []
    for _, row in stations_df.iterrows():
        key = (row['id'], DATA_TABLE)
        if key not in loaded:
            candidates.append(row)
            if len(candidates) >= batch_size:
                break
    selected_ids = [row['id'] for row in candidates]
    log_message(task_id, f"Selected stations: {selected_ids}")

    if not candidates:
        log_message(task_id, "No new stations to process.")
        con.close()
        return

    # Fetch and collect DataFrames
    dfs = []
    for row in candidates:
        sid = row['id']
        start_str = row['start_str']
        end_str   = row['end_str']
        log_message(task_id, f"Fetching {sid}: {start_str} → {end_str}")
        try:
            start = datetime.fromisoformat(start_str)
            end   = datetime.fromisoformat(end_str)
            with warnings.catch_warnings(record=True) as warns:
                warnings.simplefilter('always')
                df = Daily(sid, start, end).fetch()
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
            print(tb)
            log_message(task_id, f"Error for {sid}: {e}\nTraceback:\n{tb}")

    # Bulk insert combined DataFrame
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        con.register('df_all', combined)
        exists = con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema='main' AND table_name='{DATA_TABLE}'"
        ).fetchone()[0] > 0
        if not exists:
            con.execute(f"CREATE TABLE {DATA_TABLE} AS SELECT * FROM df_all")
        else:
            con.execute(f"INSERT INTO {DATA_TABLE} SELECT * FROM df_all")
        log_message(task_id, f"Inserted {len(combined)} rows for stations {selected_ids}")
    else:
        log_message(task_id, "No new rows to insert.")

    # Update load-log for each station
    ts_now = datetime.now(timezone.utc)
    for sid in selected_ids:
        con.execute(f"DELETE FROM {LOG_TABLE} WHERE station_id = '{sid}' AND table_name = '{DATA_TABLE}'")
        con.execute(f"INSERT INTO {LOG_TABLE} VALUES ('{sid}', TIMESTAMP '{ts_now}', '{DATA_TABLE}')")
    log_message(task_id, f"Updated load-log for stations {selected_ids}")

    con.close()

# ── DAG Definition ──────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='meteostat_daily_obs_run',
    default_args=default_args,
    description='Fetch Meteostat daily data by batch into DuckDB',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['weather', 'meteostat'],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_and_append_stations',
        python_callable=fetch_and_append_stations
    )
    fetch_task
