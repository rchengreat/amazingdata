# -*- coding: utf-8 -*-
"""
amazingdata_fetch_industry_info.py

DAG: amazingdata_fetch_industry_info
Schedule: 工作日 03:30

Tasks:
  fetch_industry_info — info_industry_basic_history.parquet（全量刷新）
                      — info_industry_detail_history.parquet（增量）
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
    dag_id="amazingdata_fetch_industry_info",
    default_args=default_args,
    schedule="30 3 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "industry", "info", "daily"],
    description="工作日 03:30 拉取 info_industry_basic_history 和 info_industry_detail_history",
) as dag:

    fetch_industry_info = BashOperator(
        task_id="fetch_industry_info",
        bash_command=DOCKER_CMD.format(script="fetch_industry_info.py"),
    )
