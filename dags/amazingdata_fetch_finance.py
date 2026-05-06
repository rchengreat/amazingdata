# -*- coding: utf-8 -*-
"""
amazingdata_fetch_finance.py

DAG: amazingdata_fetch_finance
Schedule: 工作日 05:00

Tasks（串行，各自独立 docker run）：
  fetch_balance_sheet — finance_balance_sheet_history.parquet
  fetch_cash_flow     — finance_cash_flow_history.parquet
  fetch_income        — finance_income_history.parquet

每张报表独立运行，SDK segfault 不影响其他报表。
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

_NAS_STATS = 'echo "=== NAS Stats ===" && date && cat /proc/meminfo | grep -E "MemTotal|MemAvailable" && cat /proc/net/dev | grep -v "lo:" && df -h /volume1'

_DOCKER_BASE = (
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
    "python3 scripts/fetch_finance.py --statement {statement}"
)

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="amazingdata_fetch_finance",
    default_args=default_args,
    schedule="0 5 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "finance", "daily"],
    description="工作日 05:00 依次拉取三张财务报表（各自独立 docker run）",
) as dag:

    fetch_balance_sheet = BashOperator(
        task_id="fetch_balance_sheet",
        bash_command=(
            _NAS_STATS + " && "
            + _DOCKER_BASE.format(statement="balance_sheet")
            + "; " + _NAS_STATS + "; exit 0"
        ),
        execution_timeout=timedelta(hours=3),
    )

    fetch_cash_flow = BashOperator(
        task_id="fetch_cash_flow",
        bash_command=(
            _NAS_STATS + " && "
            + _DOCKER_BASE.format(statement="cash_flow")
            + "; " + _NAS_STATS + "; exit 0"
        ),
        execution_timeout=timedelta(hours=3),
    )

    fetch_income = BashOperator(
        task_id="fetch_income",
        bash_command=(
            _NAS_STATS + " && "
            + _DOCKER_BASE.format(statement="income")
            + "; " + _NAS_STATS + "; exit 0"
        ),
        execution_timeout=timedelta(hours=3),
    )

    fetch_balance_sheet >> fetch_cash_flow >> fetch_income
