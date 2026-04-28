"""
fetch_industry_info.py — 拉取行业基础信息与成分数据

输出文件：
  - info_industry_basic_history.parquet   ← ido.get_industry_base_info（全量刷新）
  - info_industry_detail_history.parquet  ← ido.get_industry_constituent（增量：追加新 INDATE 行）

行业代码来源：从已写入的 info_industry_basic_history.parquet 的 INDEX_CODE 列读取（约 511 个）

运行时间：工作日 03:00
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


def fetch_industry_basic(ido, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 info_industry_basic_history（全量刷新，小文件）")
    df = ido.get_industry_base_info(local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_industry_base_info 返回空数据")
        return
    if isinstance(df, dict):
        df = pd.concat(list(df.values()), ignore_index=True)
    df = df.reset_index(drop=True)
    logger.info(f"拉取到 {len(df)} 行行业基础数据")
    write_parquet(df, output_dir, "info_industry_basic_history.parquet")
    logger.info("info_industry_basic_history 写入完成")


def fetch_industry_detail(ido, industry_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info(f"开始拉取 info_industry_detail_history（增量：追加新 INDATE 行），共 {len(industry_codes)} 个行业代码")
    out_path = str(Path(output_dir) / "info_industry_detail_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "INDATE") if existing is not None else None
    logger.info(f"已有最大 INDATE: {max_dt}")

    df = ido.get_industry_constituent(industry_codes, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_industry_constituent 返回空数据")
        return
    if isinstance(df, dict):
        df = pd.concat(list(df.values()), ignore_index=True)

    logger.info(f"SDK 返回 {len(df)} 行")
    result = append_new_rows(existing, df, "INDATE", max_dt)
    write_parquet(result, output_dir, "info_industry_detail_history.parquet")
    logger.info("info_industry_detail_history 写入完成")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    ido = ad.InfoData()

    fetch_industry_basic(ido, output_dir, sdk_cache_dir)

    # 从刚写入的 industry_basic 文件读取行业代码
    industry_basic_path = Path(output_dir) / "info_industry_basic_history.parquet"
    if not industry_basic_path.exists():
        logger.error("info_industry_basic_history.parquet 不存在，无法获取行业代码，跳过行业成分拉取")
        return

    df_ind = pd.read_parquet(industry_basic_path)
    industry_codes = df_ind["INDEX_CODE"].dropna().unique().tolist()
    logger.info(f"从 info_industry_basic_history 获取到 {len(industry_codes)} 个行业代码")

    fetch_industry_detail(ido, industry_codes, output_dir, sdk_cache_dir)

    logger.info("fetch_industry_info.py 全部完成")


if __name__ == "__main__":
    main()
