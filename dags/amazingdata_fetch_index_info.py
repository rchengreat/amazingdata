# -*- coding: utf-8 -*-
"""
amazingdata_fetch_index_info.py

DAG: amazingdata_fetch_index_info
Schedule: 工作日 03:30

Tasks:
  fetch_index_info — info_index_detail_history.parquet（增量）
                   — info_index_weight_history.parquet（增量）
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="amazingdata_fetch_index_info",
    default_args=default_args,
    schedule="30 3 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "index", "info", "daily"],
    description="工作日 03:30 拉取 info_index_detail_history 和 info_index_weight_history",
) as dag:

    fetch_index_info = BashOperator(
        task_id="fetch_index_info",
        bash_command=(
            "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
            "python3 /opt/airflow/scripts_ad/fetch_index_info.py"
        ),
    )
