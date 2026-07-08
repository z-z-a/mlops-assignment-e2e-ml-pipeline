"""Pipeline helpers invoked by the Airflow DAG via `uv run python -m pipeline.<module>`.

These run inside the project venv (which has boto3/mlflow), not inside the
Airflow standalone environment (which does not).
"""
