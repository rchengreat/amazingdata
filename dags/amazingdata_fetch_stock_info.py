# -*- coding: utf-8 -*-
"""
amazingdata_fetch_stock_info.py

DAG: amazingdata_fetch_stock_info
Schedule: 工作日 15:25（在行情拉取前完成）

Tasks:
  fetch_stock_basic   — info_stock_basic.parquet（增量）
  fetch_stock_factor  — info_stock_factor.parquet（全量覆写）

Script runs inside amazingdata-fetcher:latest Docker container on NAS.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ── Docker run 模板（与 NAS 手动运行命令完全一致）─────────────────
DOCKER_CMD = (
    "/usr/local/bin/docker run --rm "
    "--user 1026:100 "
    "-v /volume1/amazingdata/data:/volume1/amazingdata/data "
    "-v /volume1/amazingdata/sdk_cache:/volume1/amazingdata/sdk_cache "
    "-v /volume1/amazingdata/logs:/app/logs "
    "--env-file /opt/amazingdata/.env "
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
    dag_id="amazingdata_fetch_stock_info",
    default_args=default_args,
    schedule="25 15 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "stock", "info", "daily"],
    description="工作日 15:25 拉取 info_stock_basic 和 info_stock_factor",
) as dag:

    fetch_stock_info = BashOperator(
        task_id="fetch_stock_info",
        bash_command=DOCKER_CMD.format(script="fetch_stock_info.py"),
    )
