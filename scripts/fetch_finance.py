"""
fetch_finance.py — 拉取三张财务报表（增量模式）

包含：
  - finance_balance_sheet_history.parquet  ← get_balance_sheet（追加新 REPORTING_PERIOD 行）
  - finance_cash_flow_history.parquet      ← get_cash_flow（追加新 REPORTING_PERIOD 行）
  - finance_income_history.parquet         ← get_income（追加新 REPORTING_PERIOD 行）

增量策略：
  SDK 不支持 begin_date 过滤（验证确认参数无效），每次全量下载后客户端过滤。
  过滤基准：逐公司比较——只保留 SDK 返回中该公司 REPORTING_PERIOD 严格大于
  parquet 中已有最大值的行。避免少数滞后公司拉低全局阈值导致数据重复写入。

用法：
  python3 scripts/fetch_finance.py --statement balance_sheet|cash_flow|income
  python3 scripts/fetch_finance.py --statement balance_sheet --test-codes 000001.SZ,000002.SZ,000003.SZ,030000.SZ

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
from amazingdata_fetcher.incremental import load_existing, max_date_str
from amazingdata_fetcher.monitor import SystemMonitor

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    non_empty = [v for v in d.values() if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _append_per_code(existing: pd.DataFrame | None, new_df: pd.DataFrame,
                     date_col: str, code_col: str = "MARKET_CODE") -> pd.DataFrame:
    """
    Per-company incremental merge: for each company, only keep rows from new_df
    whose REPORTING_PERIOD is strictly greater than that company's current max in existing.

    Replaces the old global min_max cutoff, which was dragged down by a handful of
    laggard companies and caused the script to redundantly re-process rows already stored.
    """
    if existing is None:
        return new_df.reset_index(drop=True)

    # Normalise both sides to YYYYMMDD strings for comparison
    def to_str(col: pd.Series) -> pd.Series:
        if pd.api.types.is_datetime64_any_dtype(col):
            return col.dt.strftime("%Y%m%d")
        return col.astype(str).str[:8]

    existing = existing.copy()
    existing["_rp_str"] = to_str(existing[date_col])
    new_df = new_df.copy()
    new_df["_rp_str"] = to_str(new_df[date_col])

    # Per-company max date already stored
    existing_max = existing.groupby(code_col)["_rp_str"].max()

    # Keep only rows newer than the per-company max (or all rows for new companies)
    def is_new(row):
        max_dt = existing_max.get(row[code_col])
        return max_dt is None or row["_rp_str"] > max_dt

    mask = new_df.apply(is_new, axis=1)
    new_rows = new_df[mask].drop(columns=["_rp_str"])
    existing_clean = existing.drop(columns=["_rp_str"])

    logger.info(f"增量行数（逐公司比较）: {len(new_rows):,}")
    if new_rows.empty:
        return None  # sentinel: nothing to write

    result = pd.concat([existing_clean, new_rows], ignore_index=True)
    return result


def _sdk_fetch(fn, *args):
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def fetch_balance_sheet(ido, code_list: list, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_balance_sheet_history（增量：逐公司比较 REPORTING_PERIOD）")
    out_path = str(Path(output_dir) / "finance_balance_sheet_history.parquet")
    existing = load_existing(out_path)

    mon.snapshot("balance_sheet_before_sdk")
    df = _sdk_fetch(ido.get_balance_sheet, code_list, sdk_cache_dir, False)
    mon.snapshot("balance_sheet_after_sdk")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_balance_sheet 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)
    logger.info(f"SDK 返回 {len(df):,} 行")

    result = _append_per_code(existing, df, "REPORTING_PERIOD")
    if result is None:
        logger.info("无新增行，跳过写入")
        return
    write_parquet(result, output_dir, "finance_balance_sheet_history.parquet")
    logger.info(f"finance_balance_sheet_history 写入完成: {out_path} ({len(result):,} 行)")


def fetch_cash_flow(ido, code_list: list, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_cash_flow_history（增量：逐公司比较 REPORTING_PERIOD）")
    out_path = str(Path(output_dir) / "finance_cash_flow_history.parquet")
    existing = load_existing(out_path)

    mon.snapshot("cash_flow_before_sdk")
    df = _sdk_fetch(ido.get_cash_flow, code_list, sdk_cache_dir, False)
    mon.snapshot("cash_flow_after_sdk")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_cash_flow 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)
    logger.info(f"SDK 返回 {len(df):,} 行")

    result = _append_per_code(existing, df, "REPORTING_PERIOD")
    if result is None:
        logger.info("无新增行，跳过写入")
        return
    write_parquet(result, output_dir, "finance_cash_flow_history.parquet")
    logger.info(f"finance_cash_flow_history 写入完成: {out_path} ({len(result):,} 行)")


def fetch_income(ido, code_list: list, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 finance_income_history（增量：逐公司比较 REPORTING_PERIOD）")
    out_path = str(Path(output_dir) / "finance_income_history.parquet")
    existing = load_existing(out_path)

    mon.snapshot("income_before_sdk")
    df = _sdk_fetch(ido.get_income, code_list, sdk_cache_dir, False)
    mon.snapshot("income_after_sdk")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_income 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)
    logger.info(f"SDK 返回 {len(df):,} 行")

    result = _append_per_code(existing, df, "REPORTING_PERIOD")
    if result is None:
        logger.info("无新增行，跳过写入")
        return
    write_parquet(result, output_dir, "finance_income_history.parquet")
    logger.info(f"finance_income_history 写入完成: {out_path} ({len(result):,} 行)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--statement",
        choices=["balance_sheet", "cash_flow", "income"],
        default=None,
        help="拉取指定报表；不传则依次执行全部三张",
    )
    parser.add_argument(
        "--test-codes",
        nargs="+",
        default=None,
        help="测试模式：只拉取指定股票代码，例如 --test-codes 600519.SH 000001.SZ",
    )
    args = parser.parse_args()

    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    mon = SystemMonitor()
    mon.snapshot("startup")

    logger.info("登录 AmazingData...")
    get_client()
    mon.snapshot("after_login")

    bdo = ad.BaseData()
    ido = ad.InfoData()

    if args.test_codes:
        code_list = args.test_codes
        logger.info(f"测试模式，使用指定代码: {code_list}")
    else:
        code_list = bdo.get_code_list()
        logger.info(f"获取到 {len(code_list)} 个股票代码")

    all_statements = [
        ("balance_sheet", lambda: fetch_balance_sheet(ido, code_list, output_dir, sdk_cache_dir, mon)),
        ("cash_flow",     lambda: fetch_cash_flow(ido, code_list, output_dir, sdk_cache_dir, mon)),
        ("income",        lambda: fetch_income(ido, code_list, output_dir, sdk_cache_dir, mon)),
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
            mon.snapshot(f"{name}_error")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下报表拉取失败: {errors}")
    logger.info("fetch_finance.py 全部完成")
    os._exit(0)


if __name__ == "__main__":
    main()
