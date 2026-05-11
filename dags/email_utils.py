"""Shared email helpers for Airflow DAGs."""

from pathlib import Path
import logging
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import yaml

PROJECT_ROOT = "/opt/airflow"
DEFAULT_NOTIFICATION_PATHS = [
    "/opt/airflow/configs/notification_config.yaml",
    "/opt/airflow/config/notification_config.yaml",
    os.path.join(PROJECT_ROOT, "configs", "notification_config.yaml"),
    os.path.join(PROJECT_ROOT, "config", "notification_config.yaml"),
]


def load_email_config(possible_paths=None):
    """Load email configuration from known notification files."""
    paths = possible_paths or DEFAULT_NOTIFICATION_PATHS
    for path in paths:
        if Path(path).exists():
            try:
                logging.info(f"正在加载邮件配置: {path}")
                with open(path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                    return config.get("email", {})
            except Exception as e:
                logging.warning(f"加载邮件配置失败 {path}: {e}")
    logging.warning(f"未找到邮件配置文件，已尝试路径: {paths}")
    return {}


def get_recipients(dag_id, email_config=None, possible_paths=None):
    """Resolve recipients for a DAG from config."""
    if email_config is None:
        email_config = load_email_config(possible_paths)
    if not email_config:
        return []
    dag_recipients = email_config.get("dag_recipients", {})
    return dag_recipients.get(dag_id) or email_config.get("receiver_emails", [])


def build_message(subject, body, sender_email, recipients, is_html=False):
    """Build a MIME email message."""
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender_email or ""
    msg["To"] = ", ".join(recipients)
    subtype = "html" if is_html else "plain"
    msg.attach(MIMEText(body, subtype, "utf-8"))
    return msg


def send_message(msg, email_config, recipients=None):
    """Send a prepared email message."""
    recipients = recipients or []
    failed_recipients = []

    subject = msg.get("Subject", "")
    sender_email = email_config.get("sender_email", "")

    payload = None
    subtype = "plain"
    charset = "utf-8"
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        payload_bytes = part.get_payload(decode=True)
        if payload_bytes is None:
            continue
        payload = payload_bytes.decode(part.get_content_charset() or "utf-8")
        subtype = part.get_content_subtype() or "plain"
        charset = part.get_content_charset() or "utf-8"
        break

    if payload is None:
        payload = ""

    if email_config.get("use_ssl", False):
        context_ssl = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            email_config.get("smtp_server"),
            email_config.get("smtp_port"),
            context=context_ssl,
        ) as server:
            server.login(
                email_config.get("sender_email"),
                email_config.get("sender_password"),
            )
            for recipient in recipients:
                recipient_msg = MIMEMultipart()
                recipient_msg["Subject"] = subject
                recipient_msg["From"] = sender_email
                recipient_msg["To"] = recipient
                recipient_msg.attach(MIMEText(payload, subtype, charset))
                try:
                    server.send_message(recipient_msg, to_addrs=[recipient])
                except Exception as recipient_error:
                    failed_recipients.append((recipient, str(recipient_error)))
    else:
        with smtplib.SMTP(
            email_config.get("smtp_server"), email_config.get("smtp_port")
        ) as server:
            server.starttls()
            server.login(
                email_config.get("sender_email"),
                email_config.get("sender_password"),
            )
            for recipient in recipients:
                recipient_msg = MIMEMultipart()
                recipient_msg["Subject"] = subject
                recipient_msg["From"] = sender_email
                recipient_msg["To"] = recipient
                recipient_msg.attach(MIMEText(payload, subtype, charset))
                try:
                    server.send_message(recipient_msg, to_addrs=[recipient])
                except Exception as recipient_error:
                    failed_recipients.append((recipient, str(recipient_error)))

    if failed_recipients and len(failed_recipients) == len(recipients):
        raise smtplib.SMTPException(f"所有收件人发送失败: {failed_recipients}")

    if failed_recipients:
        logging.warning(f"部分收件人发送失败: {failed_recipients}")


def send_dag_alert(context):
    """Standard Airflow on_failure_callback — sends a plain-text failure alert email."""
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    execution_date = context["execution_date"].strftime('%Y-%m-%d %H:%M:%S')
    exception = context.get("exception", "未知错误")
    subject = f"⚠️ Airflow 任务失败通知 - {dag_id}"
    message = (
        f"Airflow 任务执行失败\n\n"
        f"DAG ID: {dag_id}\nTask ID: {task_id}\n"
        f"执行时间: {execution_date}\n错误信息: {exception}\n\n"
        f"请检查 Airflow 日志获取详细信息。\n"
    )
    send_email(subject, message, dag_id)


def send_email(subject, body, dag_id, is_html=False, email_config=None, possible_paths=None):
    """Load config, resolve recipients, and send an email."""
    email_config = email_config or load_email_config(possible_paths)
    if not email_config:
        logging.warning("邮件配置未加载，跳过邮件通知")
        return False

    recipients = get_recipients(dag_id, email_config)
    if not recipients:
        logging.warning("收件人列表为空，跳过邮件通知")
        return False

    msg = build_message(
        subject=subject,
        body=body,
        sender_email=email_config.get("sender_email", ""),
        recipients=recipients,
        is_html=is_html,
    )
    send_message(msg, email_config, recipients=recipients)
    return True
