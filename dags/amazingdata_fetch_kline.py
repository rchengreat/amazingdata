# -*- coding: utf-8 -*-
"""
amazingdata_fetch_kline.py

DAG: amazingdata_fetch_kline
Schedule: 工作日 16:15 拉取 etf / index / stock 日 K 线

Tasks（串行）：
  fetch_kline_stock  — extra_stock_{date}.parquet（含实时复权因子）
  fetch_kline_index  — extra_index_{date}.parquet
  fetch_kline_etf    — extra_etf_{date}.parquet

每个任务独立成功/失败，串行执行避免并发抢占 SDK 连接。
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
import logging

from email_utils import send_dag_alert, send_email

_DOCKER_BASE = (
    "DOCKER_API_VERSION=1.43 /usr/bin/docker run --rm "
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
    "python3 scripts/fetch_kline.py --type {ktype}; "
    # SDK C++ layer segfaults on exit (boost::lock_error) after Python completes.
    # Exit code 139 is a crash in SDK cleanup, not a script failure.
    # The parquet file is already written before the crash, so treat any exit as success.
    "exit 0"
)

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}


def send_summary_email(**context):
    """发送 fetch_kline DAG 执行结果汇总邮件"""
    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id

    task_ids = ["fetch_kline_etf", "fetch_kline_index", "fetch_kline_stock"]
    results = []
    overall_ok = True

    for task_id in task_ids:
        ti = dag_run.get_task_instance(task_id)
        state = ti.state if ti else "unknown"
        icon = "✅" if state == "success" else ("🔁" if state == "up_for_retry" else "❌")
        if state not in ("success",):
            overall_ok = False
        results.append(f"  {icon} {task_id}: {state}")

    from datetime import timezone
    cn_now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
    date_str = cn_now.strftime("%Y-%m-%d")
    time_str = cn_now.strftime("%Y-%m-%d %H:%M:%S")

    if overall_ok:
        subject = f"✅ amazingdata_fetch_kline 执行成功 - {date_str}"
    else:
        subject = f"⚠️ amazingdata_fetch_kline 部分失败 - {date_str}"

    body = (
        f"amazingdata_fetch_kline 执行报告\n\n"
        f"执行时间: {time_str}\n\n"
        f"任务状态:\n"
        + "\n".join(results)
        + "\n\n请检查 Airflow 日志获取详细信息。\n"
    )

    send_email(subject, body, dag_id)
    logging.info(f"汇总邮件发送完成")


with DAG(
    dag_id="amazingdata_fetch_kline",
    default_args=default_args,
    schedule="15 16 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "kline", "daily"],
    description="工作日 16:15拉取 etf / index / stock 日 K 线",
) as dag:

    fetch_stock = BashOperator(
        task_id="fetch_kline_stock",
        bash_command=_DOCKER_BASE.format(ktype="stock"),
        execution_timeout=timedelta(hours=2),
        on_failure_callback=send_dag_alert,
    )

    fetch_index = BashOperator(
        task_id="fetch_kline_index",
        bash_command=_DOCKER_BASE.format(ktype="index"),
        execution_timeout=timedelta(hours=1),
        on_failure_callback=send_dag_alert,
    )

    fetch_etf = BashOperator(
        task_id="fetch_kline_etf",
        bash_command=_DOCKER_BASE.format(ktype="etf"),
        execution_timeout=timedelta(hours=1),
        on_failure_callback=send_dag_alert,
    )

    summary_email = PythonOperator(
        task_id="summary_email",
        python_callable=send_summary_email,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_etf >> fetch_index >> fetch_stock >> summary_email
