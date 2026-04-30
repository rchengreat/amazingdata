"""
fetch_index_info.py — 拉取指数成分与权重数据

输出文件：
  - info_index_detail_history.parquet   ← ido.get_index_constituent（增量：追加新 INDATE 行）
  - info_index_weight_history.parquet   ← ido.get_index_weight（增量：追加新 TRADE_DATE 行）

指数代码来源：bdo.get_code_list('EXTRA_INDEX_A_SH_SZ')（约 624 个）
权重数据仅对 8 个主流宽基指数有效（传入所有 624 个会导致 SDK 崩溃）。

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


def fetch_index_detail(ido, index_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info(f"开始拉取 info_index_detail_history（增量：追加新 INDATE 行），共 {len(index_codes)} 个指数代码")
    out_path = str(Path(output_dir) / "info_index_detail_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "INDATE") if existing is not None else None
    logger.info(f"已有最大 INDATE: {max_dt}")

    tmp_cache = "/tmp/index_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    df = ido.get_index_constituent(index_codes, local_path=tmp_cache, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_index_constituent 返回空数据")
        return
    if isinstance(df, dict):
        df = pd.concat(list(df.values()), ignore_index=True)

    logger.info(f"SDK 返回 {len(df)} 行")
    result = append_new_rows(existing, df, "INDATE", max_dt)
    write_parquet(result, output_dir, "info_index_detail_history.parquet")
    logger.info("info_index_detail_history 写入完成")


def fetch_index_weight(ido, index_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info(f"开始拉取 info_index_weight_history（增量：追加新 TRADE_DATE 行），共 {len(index_codes)} 个指数代码")
    out_path = str(Path(output_dir) / "info_index_weight_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None
    logger.info(f"已有最大 TRADE_DATE: {max_dt}")

    # SDK bug: 传入多个代码会因内部 sort_values('TRADE_DATE') 崩溃（部分代码无该列）
    # 逐个代码单独调用后手动 concat
    tmp_cache = "/tmp/index_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    dfs = []
    for code in index_codes:
        try:
            result = ido.get_index_weight([code], local_path=tmp_cache, is_local=False)
        except Exception as e:
            logger.warning(f"get_index_weight({code}) SDK 异常: {e}，跳过")
            continue
        if result is None:
            logger.debug(f"{code}: 返回 None，跳过")
            continue
        if isinstance(result, dict):
            dfs.extend(v for v in result.values() if v is not None and not v.empty)
        elif isinstance(result, pd.DataFrame) and not result.empty:
            dfs.append(result)

    if not dfs:
        logger.error("get_index_weight 所有代码均失败，保留已有文件不覆盖")
        return

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"SDK 合并后 {len(df)} 行")

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    write_parquet(result, output_dir, "info_index_weight_history.parquet")
    logger.info("info_index_weight_history 写入完成")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    ido = ad.InfoData()

    # 约 624 个沪深指数代码
    index_codes = bdo.get_code_list("EXTRA_INDEX_A_SH_SZ")
    logger.info(f"获取到 {len(index_codes)} 个指数代码")

    fetch_index_detail(ido, index_codes, output_dir, sdk_cache_dir)

    # 权重数据仅对 8 个主流宽基指数有效（参考 extract_ad_stock.ipynb）
    major_index_codes = [
        "000016.SH", "000300.SH", "000905.SH", "000906.SH",
        "000852.SH", "000985.SH", "000688.SH", "399006.SZ",
    ]
    fetch_index_weight(ido, major_index_codes, output_dir, sdk_cache_dir)

    logger.info("fetch_index_info.py 全部完成")


if __name__ == "__main__":
    main()
