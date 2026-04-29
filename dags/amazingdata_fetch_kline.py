# -*- coding: utf-8 -*-
"""
amazingdata_fetch_kline.py

DAG: amazingdata_fetch_kline
Schedule: 工作日 15:15（ETF etf）、15:45（指数 index）、16:00（股票 stock）

Tasks（串行）：
  fetch_kline_etf    — extra_etf_{date}.parquet
  fetch_kline_index  — extra_index_{date}.parquet
  fetch_kline_stock  — extra_stock_{date}.parquet（含实时复权因子）

每个任务独立成功/失败，串行执行避免并发抢占 SDK 连接。
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

_CMD = (
    "PYTHONPATH=/opt/airflow/src_ad:/opt/airflow/src "
    "python3 /opt/airflow/scripts_ad/fetch_kline.py --type {ktype}"
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
    dag_id="amazingdata_fetch_kline",
    default_args=default_args,
    schedule="15 15 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "kline", "daily"],
    description="工作日 15:15 起依次拉取 etf / index / stock 日 K 线",
) as dag:

    fetch_etf = BashOperator(
        task_id="fetch_kline_etf",
        bash_command=_CMD.format(ktype="etf"),
        execution_timeout=timedelta(hours=1),
    )

    fetch_index = BashOperator(
        task_id="fetch_kline_index",
        bash_command=_CMD.format(ktype="index"),
        execution_timeout=timedelta(hours=1),
    )

    fetch_stock = BashOperator(
        task_id="fetch_kline_stock",
        bash_command=_CMD.format(ktype="stock"),
        execution_timeout=timedelta(hours=2),
    )

    fetch_etf >> fetch_index >> fetch_stock
