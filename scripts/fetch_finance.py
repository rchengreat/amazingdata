"""
fetch_finance.py — 拉取三张财务报表（增量模式）

包含：
  - finance_balance_sheet_history.parquet  ← get_balance_sheet（追加新 REPORTING_PERIOD 行）
  - finance_cash_flow_history.parquet      ← get_cash_flow（追加新 REPORTING_PERIOD 行）
  - finance_income_history.parquet         ← get_income（追加新 REPORTING_PERIOD 行）

运行时间：工作日 05:00 / 06:00 / 07:00
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import AmazingData as ad

from amazingdata_fetcher.client import get_client
from amazingdata_fetcher.writer import write_parquet
from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    return pd.concat(list(d.values()), ignore_index=True)


def fetch_balance_sheet(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 finance_balance_sheet_history（增量：追加新 REPORTING_PERIOD 行）...")
    out_path = str(Path(output_dir) / "finance_balance_sheet_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    df = ido.get_balance_sheet(code_list, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_balance_sheet 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_balance_sheet_history.parquet")


def fetch_cash_flow(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 finance_cash_flow_history（增量：追加新 REPORTING_PERIOD 行）...")
    out_path = str(Path(output_dir) / "finance_cash_flow_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    df = ido.get_cash_flow(code_list, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_cash_flow 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_cash_flow_history.parquet")


def fetch_income(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 finance_income_history（增量：追加新 REPORTING_PERIOD 行）...")
    out_path = str(Path(output_dir) / "finance_income_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    df = ido.get_income(code_list, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_income 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_income_history.parquet")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    ido = ad.InfoData()

    code_list = bdo.get_code_list()
    logger.info(f"获取到 {len(code_list)} 个股票代码")

    fetch_balance_sheet(ido, code_list, output_dir, sdk_cache_dir)
    fetch_cash_flow(ido, code_list, output_dir, sdk_cache_dir)
    fetch_income(ido, code_list, output_dir, sdk_cache_dir)

    logger.info("fetch_finance.py 全部完成")


if __name__ == "__main__":
    main()
