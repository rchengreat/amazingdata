# -*- coding: utf-8 -*-
"""
amazingdata_fetch_margin.py

DAG: amazingdata_fetch_margin
Schedule: 工作日 18:00

Tasks:
  fetch_margin   — margin_summary_history.parquet（增量）
                 — margin_detail_history.parquet（增量）
  summary_email  — 发送执行报告邮件
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

_DOCKER_CMD = (
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
    "python3 scripts/fetch_margin.py"
)

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
    msgs = []
    for line in output.splitlines():
        m = re.search(r"\| \w+\s+\| .+? - (.+)", line)
        if m:
            msgs.append(m.group(1).strip())
        elif line.strip():
            msgs.append(line.strip())
    return msgs


def _file_info(filename: str) -> str:
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


def _extract_detail(msgs: list[str], filename: str) -> tuple[str, str]:
    """
    Return (status, detail) for one output file based on parsed log messages.
    status: 'success' | 'skipped' | 'failed'
    """
    file_path = Path(_DATA_DIR) / filename
    error_lines = [m for m in msgs if "错误" in m or "ERROR" in m or "Exception" in m or "Traceback" in m or "失败" in m]
    skip_lines = [m for m in msgs if "无新增行" in m or "跳过写入" in m]
    write_lines = [m for m in msgs if "写入完成" in m and filename.split(".")[0].replace("_history", "") in m]

    if write_lines:
        return "success", f"{filename}: {_file_info(filename)}"
    elif skip_lines:
        # Find the most relevant skip message
        skip_msg = next((m for m in msgs if "无新增行" in m or "跳过写入" in m), "无新增行，跳过写入")
        return "skipped", f"{filename}: {skip_msg}"
    elif file_path.exists():
        # File exists from a previous run but wasn't updated today
        return "skipped", f"{filename}: 已有文件未更新（{_file_info(filename)}）"
    else:
        detail = f"{filename} 未生成"
        if error_lines:
            detail += "\n    原因: " + "; ".join(error_lines[:3])
        return "failed", detail


def run_fetch_margin(**context):
    ti = context["task_instance"]

    proc = subprocess.run(
        _DOCKER_CMD, shell=True, capture_output=True, text=True,
        env={**os.environ, "DOCKER_API_VERSION": "1.43"},
    )
    output = proc.stdout + proc.stderr
    for line in output.splitlines():
        logging.info(f"[docker] {line}")

    msgs = _parse_log_lines(output)

    results = {}
    for fname in ["margin_summary_history.parquet", "margin_detail_history.parquet"]:
        status, detail = _extract_detail(msgs, fname)
        results[fname] = {"status": status, "detail": detail}

    ti.xcom_push(key="result", value=results)

    failed = [f for f, r in results.items() if r["status"] == "failed"]
    if failed:
        raise RuntimeError(f"fetch_margin 部分失败: {failed}")


def send_summary_email(**context):
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id

    cn_now = datetime.now(timezone.utc).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Shanghai")
    )
    date_str = cn_now.strftime("%Y-%m-%d")
    time_str = cn_now.strftime("%Y-%m-%d %H:%M:%S")

    results = ti.xcom_pull(task_ids="fetch_margin", key="result") or {}

    rows = []
    overall_ok = True
    for fname, r in results.items():
        status = r["status"]
        detail = r["detail"]
        if status == "success":
            icon = "✅"
        elif status == "skipped":
            icon = "⏭️"
        else:
            icon = "❌"
            overall_ok = False
        rows.append(f"  {icon} {detail}")

    if not rows:
        overall_ok = False
        rows = ["  ❌ 任务异常退出，无详细信息"]

    subject = (
        f"✅ amazingdata_fetch_margin 执行成功 - {date_str}"
        if overall_ok
        else f"⚠️ amazingdata_fetch_margin 部分失败 - {date_str}"
    )

    body = (
        f"amazingdata_fetch_margin 执行报告\n\n"
        f"执行时间: {time_str}\n\n"
        f"文件结果:\n"
        + "\n".join(rows)
        + "\n\n请检查 Airflow 日志获取详细信息。\n"
    )

    send_email(subject, body, dag_id)
    logging.info("汇总邮件发送完成")


with DAG(
    dag_id="amazingdata_fetch_margin",
    default_args=default_args,
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "margin", "daily"],
    description="工作日 18:00拉取 margin_summary_history 和 margin_detail_history",
) as dag:

    fetch_margin = PythonOperator(
        task_id="fetch_margin",
        python_callable=run_fetch_margin,
        on_failure_callback=send_dag_alert,
    )

    summary_email = PythonOperator(
        task_id="summary_email",
        python_callable=send_summary_email,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_margin >> summary_email
