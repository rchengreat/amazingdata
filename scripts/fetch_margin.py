"""
fetch_margin.py — 拉取融资融券数据（增量模式）

包含：
  - margin_summary_history.parquet  ← get_margin_summary（追加新 TRADE_DATE 行）
  - margin_detail_history.parquet   ← get_margin_detail（追加新 TRADE_DATE 行）

运行时间：工作日 16:15 / 16:30
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


def _sdk_fetch(fn, *args):
    """Wrap SDK call to catch HDF5/tables cache errors without aborting."""
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def fetch_margin_summary(ido, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_summary_history（增量：追加新 TRADE_DATE 行）")
    out_path = str(Path(output_dir) / "margin_summary_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

    df = _sdk_fetch(ido.get_margin_summary, sdk_cache_dir, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_margin_summary 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    write_parquet(result, output_dir, "margin_summary_history.parquet")
    logger.info("margin_summary_history 写入完成")


def fetch_margin_detail(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_detail_history（增量：追加新 TRADE_DATE 行）")
    out_path = str(Path(output_dir) / "margin_detail_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

    df = _sdk_fetch(ido.get_margin_detail, code_list, sdk_cache_dir, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_margin_detail 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    write_parquet(result, output_dir, "margin_detail_history.parquet")
    logger.info("margin_detail_history 写入完成")


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

    errors = []
    for name, fn in [
        ("margin_summary", lambda: fetch_margin_summary(ido, output_dir, sdk_cache_dir)),
        ("margin_detail",  lambda: fetch_margin_detail(ido, code_list, output_dir, sdk_cache_dir)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下数据拉取失败: {errors}")
    logger.info("fetch_margin.py 全部完成")


if __name__ == "__main__":
    main()
