# -*- coding: utf-8 -*-
"""
amazingdata_fetch_stock_info.py

DAG: amazingdata_fetch_stock_info
Schedule: 工作日 03:00

Tasks:
  fetch_stock_info — info_stock_basic.parquet（增量）
                   — info_stock_factor.parquet（全量覆写）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

_CMD = (
    "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
    "python3 /opt/airflow/scripts_ad/{script}"
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
    schedule="0 3 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "stock", "info", "daily"],
    description="工作日 03:00 拉取 info_stock_basic 和 info_stock_factor",
) as dag:

    fetch_stock_info = BashOperator(
        task_id="fetch_stock_info",
        bash_command=_CMD.format(script="fetch_stock_info.py"),
    )
