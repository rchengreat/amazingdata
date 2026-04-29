# -*- coding: utf-8 -*-
"""
amazingdata_monthly_cleanup.py

DAG: amazingdata_monthly_cleanup
Schedule: 每月 2 日 01:00

Tasks:
  monthly_cleanup — 合并上月每日 extra_{type}_{date}.parquet → extra_{type}_history.parquet
                   删除已合并的日期文件
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
    dag_id="amazingdata_monthly_cleanup",
    default_args=default_args,
    schedule="0 1 2 * *",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "cleanup", "monthly"],
    description="每月 2 日 01:00 合并日 K 线文件为月度 history 文件",
) as dag:

    monthly_cleanup = BashOperator(
        task_id="monthly_cleanup",
        bash_command=(
            "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
            "python3 /opt/airflow/scripts_ad/monthly_cleanup.py"
        ),
    )
