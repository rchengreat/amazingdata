# -*- coding: utf-8 -*-
"""
amazingdata_fetch_finance.py

DAG: amazingdata_fetch_finance
Schedule: 工作日 05:00

Tasks:
  fetch_finance — finance_balance_sheet_history.parquet（增量）
               — finance_cash_flow_history.parquet（增量）
               — finance_income_history.parquet（增量）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DOCKER_CMD = (
    "sudo /usr/local/bin/docker run --rm "
    "--user 1026:100 "
    "-v /volume1/amazingdata/data:/volume1/amazingdata/data "
    "-v /volume1/amazingdata/sdk_cache:/volume1/amazingdata/sdk_cache "
    "-v /volume1/amazingdata/logs:/app/logs "
    "--env-file /volume1/amazingdata/.env "
    "-e NUMBA_CACHE_DIR=/tmp/numba_cache "
    "amazingdata-fetcher:latest "
    "python3 scripts/{script}"
)

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="amazingdata_fetch_finance",
    default_args=default_args,
    schedule="0 5 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "finance", "daily"],
    description="工作日 05:00 拉取三张财务报表",
) as dag:

    fetch_finance = BashOperator(
        task_id="fetch_finance",
        bash_command=DOCKER_CMD.format(script="fetch_finance.py"),
    )
