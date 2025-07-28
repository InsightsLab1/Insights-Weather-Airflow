from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone, date
import os
import duckdb
import warnings
import traceback
from meteostat import Daily

# ── Configuration ─────────────────────────────────────────────────────────
DEFAULT_COUNTRIES  = ['US']   # Empty list means all countries
DEFAULT_STATES     = []       # Empty list means all states/provinces
DEFAULT_BATCH_SIZE = 5       # Number of stations to update per run

# ── Paths and Tables ──────────────────────────────────────────────────────
LOG_PATH      = '/opt/airflow/data/data_log'
DUCKDB_PATH    = '/opt/airflow/data/climate_dwh.db'
STATION_TABLE = 'land_obs_stations'  # metadata with daily_start/daily_end
DATA_TABLE    = 'land_obs_daily'     # existing daily observations
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_message(task_id: str, message: str):
    os.makedirs(LOG_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    path = os.path.join(LOG_PATH, f"{task_id}.log")
    with open(path, 'a') as f:
        f.write(f"[{ts}] {message}\n")


def update_daily_records(**kwargs):
    task_id = kwargs['task'].task_id
    log_message(task_id, 'Starting update_daily_records')

    # read conf
    conf = getattr(kwargs.get('dag_run'), 'conf', {}) or {}
    countries  = conf.get('countries') or DEFAULT_COUNTRIES
    states     = conf.get('states') or DEFAULT_STATES
    batch_size = int(conf.get('batch_size', DEFAULT_BATCH_SIZE))
    log_message(task_id, f"Config - countries: {countries}, states: {states}, batch_size: {batch_size}")

    # connect
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    # load station metadata
    query = f"SELECT id, country, region, daily_start, daily_end FROM {STATION_TABLE}"
    stations = con.execute(query).df()
    log_message(task_id, f"Loaded {len(stations)} station metadata rows")

    # filter metadata
    stations = stations.dropna(subset=['daily_start', 'daily_end'])
    if countries:
        stations = stations[stations['country'].isin(countries)]
    if states:
        stations = stations[stations['region'].isin(states)]
    log_message(task_id, f"{len(stations)} stations after filtering")

    # get last loaded per station
    max_dates = con.execute(
        f"SELECT station_id, MAX(time) AS last_date FROM {DATA_TABLE} GROUP BY station_id"
    ).df()
    last_loaded = dict(zip(max_dates['station_id'], max_dates['last_date']))
    log_message(task_id, f"Retrieved last loaded dates for {len(last_loaded)} stations")

    # pick candidates needing update
    candidates = []
    for _, row in stations.iterrows():
        sid = row['id']
        # determine start
        if sid in last_loaded:
            start_dt = last_loaded[sid] + timedelta(days=1)
        else:
            start_dt = datetime.fromisoformat(row['daily_start'])
        end_dt = min(datetime.fromisoformat(row['daily_end']), datetime.now(timezone.utc))
        if start_dt.date() > end_dt.date():
            continue
        candidates.append((sid, start_dt, end_dt))
        if len(candidates) >= batch_size:
            break

    log_message(task_id, f"Stations to update: {[c[0] for c in candidates]}")
    if not candidates:
        log_message(task_id, 'No stations require update')
        con.close()
        return

    # update loop
    for sid, start_dt, end_dt in candidates:
        log_message(task_id, f"Updating {sid}: {start_dt.date()} → {end_dt.date()}")
        try:
            with warnings.catch_warnings(record=True) as warns:
                warnings.simplefilter('always')
                df = Daily(sid, start_dt, end_dt).fetch()
            for w in warns:
                log_message(task_id, f"Warning for {sid}: {w.message}")

            if df is not None and hasattr(df, 'empty') and not df.empty:
                df = df.reset_index()
                df['station_id'] = sid
                ts_now = datetime.now(timezone.utc)
                df['load_time'] = ts_now.isoformat()
                con.register('tmp', df)
                exists = con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_schema='main' AND table_name='{DATA_TABLE}'"
                ).fetchone()[0] > 0
                if exists:
                    con.execute(f"INSERT INTO {DATA_TABLE} SELECT * FROM tmp")
                else:
                    con.execute(f"CREATE TABLE {DATA_TABLE} AS SELECT * FROM tmp")
                log_message(task_id, f"Inserted {len(df)} new rows for {sid}")
            else:
                log_message(task_id, f"No new records for {sid}")

        except Exception as e:
            tb = traceback.format_exc()
            print(tb)
            log_message(task_id, f"Error updating {sid}: {e}\n{tb}")

    con.close()

with DAG(
    dag_id='meteostat_daily_obs_update',
    default_args=default_args,
    description='Incrementally update daily observations from Meteostat → DuckDB',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['weather', 'meteostat'],
) as dag:
    update_task = PythonOperator(
        task_id='update_daily_records',
        python_callable=update_daily_records
    )
    update_task
