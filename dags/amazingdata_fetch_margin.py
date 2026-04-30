# -*- coding: utf-8 -*-
"""
amazingdata_fetch_margin.py

DAG: amazingdata_fetch_margin
Schedule: 工作日 15:45

Tasks:
  fetch_margin — margin_summary_history.parquet（增量）
              — margin_detail_history.parquet（增量）
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
    "-e AD_HOST "
    "-e AD_PORT "
    "-e AD_USERNAME "
    "-e AD_PASSWORD "
    "-e OUTPUT_DIR=/volume1/amazingdata/data "
    "-e SDK_CACHE_DIR=/volume1/amazingdata/sdk_cache/ "
    "-e PYTHONPATH=/app/src "
    "-e HOME=/tmp "
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
    dag_id="amazingdata_fetch_margin",
    default_args=default_args,
    schedule="45 15 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "margin", "daily"],
    description="工作日 15:45 拉取 margin_summary_history 和 margin_detail_history",
) as dag:

    fetch_margin = BashOperator(
        task_id="fetch_margin",
        bash_command=DOCKER_CMD.format(script="fetch_margin.py"),
    )
