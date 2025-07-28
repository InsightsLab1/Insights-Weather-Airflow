from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import duckdb
import warnings
import traceback
from meteostat import Hourly

# ── Configuration ─────────────────────────────────────────────────────────
DEFAULT_COUNTRIES  = ['US']    # Empty list = all countries
DEFAULT_STATES     = []        # Empty list = all states/provinces
DEFAULT_BATCH_SIZE = 3         # Number of stations per update run

# ── Paths and Tables ──────────────────────────────────────────────────────
LOG_PATH       = '/opt/airflow/data/data_log'
DUCKDB_PATH     = '/opt/airflow/data/climate_dwh.db'
STATION_TABLE  = 'land_obs_stations'   # metadata (with hourly_start/hourly_end)
HOURLY_TABLE   = 'land_obs_hourly'     # target hourly observations
LOG_TABLE      = 'land_obs_load_log'   # shared load-tracking table

# ── Helper: Logging ───────────────────────────────────────────────────────
def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    path = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(path, 'a') as f:
        f.write(f"[{now}] {message}\n")

# ── Core Task: Incremental Hourly Update ───────────────────────────────────
def update_hourly_records(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, 'Starting update_hourly_records')

    # Read run-time config
    conf = getattr(kwargs.get('dag_run'), 'conf', {}) or {}
    countries  = conf.get('countries') or DEFAULT_COUNTRIES
    states     = conf.get('states')    or DEFAULT_STATES
    batch_size = int(conf.get('batch_size', DEFAULT_BATCH_SIZE))
    log_message(task_id, f"Config: countries={countries}, states={states}, batch_size={batch_size}")

    # Prepare DuckDB
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    # Load station metadata with hourly bounds
    meta_q = (
        f"SELECT id, country, region, hourly_start, hourly_end "
        f"FROM {STATION_TABLE}"
    )
    stations = con.execute(meta_q).df()
    log_message(task_id, f"Loaded {len(stations)} stations from {STATION_TABLE}")

    # Filter by bounds and config
    stations = stations.dropna(subset=['hourly_start', 'hourly_end'])
    if countries:
        stations = stations[stations['country'].isin(countries)]
    if states:
        stations = stations[stations['region'].isin(states)]
    log_message(task_id, f"{len(stations)} stations after filtering by country/state")

    # Determine last loaded timestamp per station
    last_df = con.execute(
        f"SELECT station_id, MAX(time) AS last_time FROM {HOURLY_TABLE} GROUP BY station_id"
    ).df()
    last_loaded = dict(zip(last_df['station_id'], last_df['last_time']))
    log_message(task_id, f"Found last loaded hourly for {len(last_loaded)} stations")

    # Pick batch candidates needing update
    candidates = []
    for _, row in stations.iterrows():
        sid = row['id']
        if sid in last_loaded and last_loaded[sid] is not None:
            start_ts = last_loaded[sid] + timedelta(hours=1)
        else:
            start_ts = datetime.fromisoformat(row['hourly_start'])
        meta_end = datetime.fromisoformat(row['hourly_end'])
        end_ts = min(meta_end, datetime.now(timezone.utc))
        if start_ts > end_ts:
            continue
        candidates.append((sid, start_ts, end_ts))
        if len(candidates) >= batch_size:
            break
    log_message(task_id, f"Stations to update: {[sid for sid, _, _ in candidates]}")

    if not candidates:
        log_message(task_id, 'No stations require hourly update')
        con.close()
        return

    # Update loop
    for sid, start_ts, end_ts in candidates:
        log_message(task_id, f"Updating station {sid}: {start_ts} → {end_ts}")
        try:
            # Fetch hourly data
            with warnings.catch_warnings(record=True) as warns:
                warnings.simplefilter('always')
                df = Hourly(sid, start_ts, end_ts).fetch()
            for w in warns:
                log_message(task_id, f"Warning for {sid}: {w.message}")

            ts_now = datetime.now(timezone.utc)
            if df is not None and hasattr(df, 'empty') and not df.empty:
                df = df.reset_index()
                df['station_id'] = sid
                df['load_time']  = ts_now.isoformat()
                con.register('tmp_df', df)
                exists = con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_schema='main' AND table_name='{HOURLY_TABLE}'"
                ).fetchone()[0] > 0
                if not exists:
                    con.execute(f"CREATE TABLE {HOURLY_TABLE} AS SELECT * FROM tmp_df")
                else:
                    con.execute(f"INSERT INTO {HOURLY_TABLE} SELECT * FROM tmp_df")
                log_message(task_id, f"Inserted {len(df)} new hourly rows for {sid}")
            else:
                log_message(task_id, f"No new hourly records for {sid}")

            # Update metadata hourly_end
            new_end = end_ts.isoformat()
            con.execute(
                f"UPDATE {STATION_TABLE} SET hourly_end = '{new_end}' WHERE id = '{sid}'"
            )
            log_message(task_id, f"Updated hourly_end for {sid} to {new_end}")

            # Update load log
            con.execute(
                f"DELETE FROM {LOG_TABLE} WHERE station_id = '{sid}' AND table_name = '{HOURLY_TABLE}'"
            )
            con.execute(
                f"INSERT INTO {LOG_TABLE} VALUES ('{sid}', TIMESTAMP '{ts_now}', '{HOURLY_TABLE}')"
            )

        except Exception as e:
            tb = traceback.format_exc()
            print(tb)
            log_message(task_id, f"Error updating hourly for {sid}: {e}\nTraceback:\n{tb}")

    con.close()

# ── DAG Definition ──────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='meteostat_hourly_obs_update',
    default_args=default_args,
    description='Incrementally update hourly observations from Meteostat → DuckDB',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['weather', 'meteostat'],
) as dag:
    update_task = PythonOperator(
        task_id='update_hourly_records',
        python_callable=update_hourly_records
    )
    update_task
