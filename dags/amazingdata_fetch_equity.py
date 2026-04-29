# -*- coding: utf-8 -*-
"""
amazingdata_fetch_equity.py

DAG: amazingdata_fetch_equity
Schedule: 工作日 04:30

Tasks:
  fetch_equity — equity_structure_history.parquet（增量）
               — equity_dividend_history.parquet（增量）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DOCKER_CMD = (
    "/usr/local/bin/docker run --rm "
    "--user 1026:100 "
    "-v /volume1/amazingdata/data:/volume1/amazingdata/data "
    "-v /volume1/amazingdata/sdk_cache:/volume1/amazingdata/sdk_cache "
    "-v /volume1/amazingdata/logs:/app/logs "
    "-e AD_HOST -e AD_PORT -e AD_USERNAME -e AD_PASSWORD "\
    "-e OUTPUT_DIR=$AD_OUTPUT_DIR -e SDK_CACHE_DIR=$AD_SDK_CACHE_DIR "
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
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="amazingdata_fetch_equity",
    default_args=default_args,
    schedule="30 4 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "equity", "daily"],
    description="工作日 04:30 拉取 equity_structure_history 和 equity_dividend_history",
) as dag:

    fetch_equity = BashOperator(
        task_id="fetch_equity",
        bash_command=DOCKER_CMD.format(script="fetch_equity.py"),
    )
