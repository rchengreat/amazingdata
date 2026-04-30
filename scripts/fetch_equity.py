"""
fetch_equity.py — 拉取股本结构与分红数据（增量模式）

包含：
  - equity_structure_history.parquet  ← get_equity_structure（追加新 CHANGE_DATE 行）
  - equity_dividend_history.parquet   ← get_dividend（追加新 DATE_EQY_RECORD 行）

运行时间：工作日 04:30 / 04:45
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


def fetch_equity_structure(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 equity_structure_history（增量：追加新 CHANGE_DATE 行）...")
    out_path = str(Path(output_dir) / "equity_structure_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "CHANGE_DATE") if existing is not None else None

    tmp_cache = "/tmp/equity_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = ido.get_equity_structure(code_list, local_path=tmp_cache, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_equity_structure 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "CHANGE_DATE", max_dt)
    write_parquet(result, output_dir, "equity_structure_history.parquet")


def fetch_equity_dividend(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 equity_dividend_history（增量：追加新 DATE_EQY_RECORD 行）...")
    out_path = str(Path(output_dir) / "equity_dividend_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "DATE_EQY_RECORD") if existing is not None else None

    tmp_cache = "/tmp/equity_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = ido.get_dividend(code_list, local_path=tmp_cache, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_dividend 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "DATE_EQY_RECORD", max_dt)
    write_parquet(result, output_dir, "equity_dividend_history.parquet")


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

    fetch_equity_structure(ido, code_list, output_dir, sdk_cache_dir)
    fetch_equity_dividend(ido, code_list, output_dir, sdk_cache_dir)

    logger.info("fetch_equity.py 全部完成")


if __name__ == "__main__":
    main()
