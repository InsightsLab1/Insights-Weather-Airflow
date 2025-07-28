# Airflow

things to improve:
Modularize shared utilities
You’ve duplicated logging and connection boilerplate across DAGs. In the future, we could extract those into a shared Python module (e.g. utils/logging.py, utils/duckdb.py) so that each DAG file stays focused on just the task logic.

Parameterize environments
If you ever promote this to multiple environments (dev, staging, prod), having your paths, table names, and defaults driven by Airflow Variables or a config file can minimize copy-paste changes.

Add automated tests
A few small pytest suites that mock out Daily.fetch() and verify your insert/append logic can catch edge cases early (e.g. empty DataFrames, permission errors).

Documentation & README
A short markdown doc outlining how to trigger these DAGs, what conf keys they accept, and where logs land will make ramp-up easier for anyone new.