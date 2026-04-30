"""
fetch_finance.py — 拉取三张财务报表（增量模式）

包含：
  - finance_balance_sheet_history.parquet  ← get_balance_sheet（追加新 REPORTING_PERIOD 行）
  - finance_cash_flow_history.parquet      ← get_cash_flow（追加新 REPORTING_PERIOD 行）
  - finance_income_history.parquet         ← get_income（追加新 REPORTING_PERIOD 行）

用法：
  python3 fetch_finance.py --statement balance_sheet|cash_flow|income
  （不传则依次执行全部三张，但 SDK 可能在中途 segfault 导致后续报表丢失）

运行时间：工作日 05:00 / 06:00 / 07:00
"""
import argparse
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


def _sdk_fetch(fn, *args):
    """Call an SDK method and return the DataFrame, or None on error.
    Catches HDF5/tables errors from SDK cache writes so one statement
    failure does not abort the others."""
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def fetch_balance_sheet(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_balance_sheet_history（增量：追加新 REPORTING_PERIOD 行）")
    out_path = str(Path(output_dir) / "finance_balance_sheet_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    tmp_cache = "/tmp/finance_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = _sdk_fetch(ido.get_balance_sheet, code_list, tmp_cache, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_balance_sheet 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_balance_sheet_history.parquet")
    logger.info("finance_balance_sheet_history 写入完成")


def fetch_cash_flow(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_cash_flow_history（增量：追加新 REPORTING_PERIOD 行）")
    out_path = str(Path(output_dir) / "finance_cash_flow_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    tmp_cache = "/tmp/finance_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = _sdk_fetch(ido.get_cash_flow, code_list, tmp_cache, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_cash_flow 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_cash_flow_history.parquet")
    logger.info("finance_cash_flow_history 写入完成")


def fetch_income(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_income_history（增量：追加新 REPORTING_PERIOD 行）")
    out_path = str(Path(output_dir) / "finance_income_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "REPORTING_PERIOD") if existing is not None else None

    tmp_cache = "/tmp/finance_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = _sdk_fetch(ido.get_income, code_list, tmp_cache, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_income 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "REPORTING_PERIOD", max_dt)
    write_parquet(result, output_dir, "finance_income_history.parquet")
    logger.info("finance_income_history 写入完成")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--statement",
        choices=["balance_sheet", "cash_flow", "income"],
        default=None,
        help="拉取指定报表；不传则依次执行全部三张",
    )
    args = parser.parse_args()

    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    ido = ad.InfoData()

    code_list = bdo.get_code_list()
    logger.info(f"获取到 {len(code_list)} 个股票代码")

    all_statements = [
        ("balance_sheet", lambda: fetch_balance_sheet(ido, code_list, output_dir, sdk_cache_dir)),
        ("cash_flow",     lambda: fetch_cash_flow(ido, code_list, output_dir, sdk_cache_dir)),
        ("income",        lambda: fetch_income(ido, code_list, output_dir, sdk_cache_dir)),
    ]

    if args.statement:
        statements = [(n, fn) for n, fn in all_statements if n == args.statement]
    else:
        statements = all_statements

    errors = []
    for name, fn in statements:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下报表拉取失败: {errors}")
    logger.info("fetch_finance.py 全部完成")


if __name__ == "__main__":
    main()
