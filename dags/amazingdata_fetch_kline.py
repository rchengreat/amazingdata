# -*- coding: utf-8 -*-
"""
amazingdata_fetch_kline.py

DAG: amazingdata_fetch_kline
Schedule: 工作日 17:15 拉取 etf / index / stock 日 K 线

Tasks（串行）：
  fetch_kline_etf    — extra_etf_{date}.parquet
  fetch_kline_index  — extra_index_{date}.parquet
  fetch_kline_stock  — extra_stock_{date}.parquet（含复权因子）
  summary_email      — 发送执行报告邮件
"""

import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

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
    "python3 scripts/fetch_kline.py --type {ktype}"
)

# Inside the Airflow container, /volume1/amazingdata/data is mounted here
_DATA_DIR = "/opt/airflow/tgw_data"

default_args = {
    "owner": "rollandchen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}


def _parse_log_lines(output: str) -> list[str]:
    """Extract the message portion from loguru-formatted lines."""
    msgs = []
    for line in output.splitlines():
        # loguru format: "2026-05-11 09:06:08.536 | INFO     | __main__:fn:N - message"
        m = re.search(r"\| \w+\s+\| .+? - (.+)", line)
        if m:
            msgs.append(m.group(1).strip())
        elif line.strip():
            msgs.append(line.strip())
    return msgs


def _file_info(filename: str) -> str:
    """Return file size and row count string, or 'not found'."""
    path = Path(_DATA_DIR) / filename
    if not path.exists():
        return "not found"
    size_mb = path.stat().st_size / 1_048_576
    try:
        import pandas as pd
        rows = len(pd.read_parquet(path, columns=[pd.read_parquet(path).columns[0]]))
        return f"{rows:,} rows, {size_mb:.1f} MB"
    except Exception:
        return f"{size_mb:.1f} MB"


def _run_kline(ktype: str, **context):
    """Run docker fetch_kline for one type, capture output, push XCom result."""
    ti = context["task_instance"]
    trade_date = datetime.now(timezone.utc).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    ).strftime("%Y%m%d")
    filename = f"extra_{ktype}_{trade_date}.parquet"

    cmd = _DOCKER_BASE.format(ktype=ktype)
    logging.info(f"Running: {cmd}")

    proc = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        env={**os.environ, "DOCKER_API_VERSION": "1.43"},
    )
    output = proc.stdout + proc.stderr
    for line in output.splitlines():
        logging.info(f"[docker] {line}")

    msgs = _parse_log_lines(output)

    # Determine outcome by checking if the file was created
    file_path = Path(_DATA_DIR) / filename
    already_existed = any("已存在，跳过" in m for m in msgs)
    error_lines = [m for m in msgs if "错误" in m or "ERROR" in m or "Exception" in m or "Traceback" in m]

    if already_existed:
        status = "skipped"
        detail = f"{filename} 已存在，跳过"
    elif file_path.exists():
        status = "success"
        detail = f"{filename}: {_file_info(filename)}"
    else:
        status = "failed"
        detail = f"{filename} 未生成"
        if error_lines:
            detail += "\n    原因: " + "; ".join(error_lines[:3])

    result = {"status": status, "filename": filename, "detail": detail}
    ti.xcom_push(key="result", value=result)

    # SDK segfaults on exit with code 139 — that's cleanup, not a script failure.
    # Only raise if the file wasn't produced and there's no skip.
    if status == "failed":
        raise RuntimeError(f"fetch_kline --type {ktype} failed: {detail}")


def fetch_kline_etf(**context):
    _run_kline("etf", **context)


def fetch_kline_index(**context):
    _run_kline("index", **context)


def fetch_kline_stock(**context):
    _run_kline("stock", **context)


def send_summary_email(**context):
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id

    cn_now = datetime.now(timezone.utc).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    )
    date_str = cn_now.strftime("%Y-%m-%d")
    time_str = cn_now.strftime("%Y-%m-%d %H:%M:%S")

    task_ids = ["fetch_kline_etf", "fetch_kline_index", "fetch_kline_stock"]
    rows = []
    overall_ok = True

    for task_id in task_ids:
        result = ti.xcom_pull(task_ids=task_id, key="result")
        if result:
            status = result["status"]
            detail = result["detail"]
        else:
            # Task didn't push XCom — it failed before reaching that point
            status = "failed"
            detail = "任务异常退出，无详细信息"

        if status == "success":
            icon = "✅"
        elif status == "skipped":
            icon = "⏭️"
        else:
            icon = "❌"
            overall_ok = False

        rows.append(f"  {icon} {task_id}: {detail}")

    subject = (
        f"✅ amazingdata_fetch_kline 执行成功 - {date_str}"
        if overall_ok
        else f"⚠️ amazingdata_fetch_kline 部分失败 - {date_str}"
    )

    body = (
        f"amazingdata_fetch_kline 执行报告\n\n"
        f"执行时间: {time_str}\n\n"
        f"任务结果:\n"
        + "\n".join(rows)
        + "\n\n请检查 Airflow 日志获取详细信息。\n"
    )

    send_email(subject, body, dag_id)
    logging.info("汇总邮件发送完成")


with DAG(
    dag_id="amazingdata_fetch_kline",
    default_args=default_args,
    schedule="15 17 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "kline", "daily"],
    description="工作日 17:15拉取 etf / index / stock 日 K 线",
) as dag:

    fetch_etf = PythonOperator(
        task_id="fetch_kline_etf",
        python_callable=fetch_kline_etf,
        execution_timeout=timedelta(hours=1),
        on_failure_callback=send_dag_alert,
    )

    fetch_index = PythonOperator(
        task_id="fetch_kline_index",
        python_callable=fetch_kline_index,
        execution_timeout=timedelta(hours=1),
        on_failure_callback=send_dag_alert,
    )

    fetch_stock = PythonOperator(
        task_id="fetch_kline_stock",
        python_callable=fetch_kline_stock,
        execution_timeout=timedelta(hours=2),
        on_failure_callback=send_dag_alert,
    )

    summary_email = PythonOperator(
        task_id="summary_email",
        python_callable=send_summary_email,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_etf >> fetch_index >> fetch_stock >> summary_email
