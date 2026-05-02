"""
fetch_equity.py — 拉取股本结构与分红数据（全量覆写）

包含：
  - equity_structure_history.parquet  ← get_equity_structure（全量覆写）
  - equity_dividend_history.parquet   ← get_dividend（全量覆写）

说明：
  SDK 不支持增量过滤，每次全量下载后直接覆写。
  不读取已有 parquet 文件。

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

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    non_empty = [v for v in d.values() if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def fetch_equity_structure(ido, code_list: list, output_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 equity_structure_history（全量覆写）")
    tmp_cache = "/tmp/equity_structure_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)

    try:
        df = ido.get_equity_structure(code_list, local_path=tmp_cache, is_local=False)
    except Exception as e:
        logger.error(f"get_equity_structure 异常: {type(e).__name__}: {e}")
        return

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_equity_structure 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    logger.info(f"SDK 返回 {len(df):,} 行")
    out_path = str(Path(output_dir) / "equity_structure_history.parquet")
    write_parquet(df, output_dir, "equity_structure_history.parquet")
    logger.info(f"equity_structure_history 写入完成: {out_path} ({len(df):,} 行)")


def fetch_equity_dividend(ido, code_list: list, output_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 equity_dividend_history（全量覆写）")
    tmp_cache = "/tmp/equity_dividend_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)

    try:
        df = ido.get_dividend(code_list, local_path=tmp_cache, is_local=False)
    except Exception as e:
        logger.error(f"get_dividend 异常: {type(e).__name__}: {e}")
        return

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_dividend 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    logger.info(f"SDK 返回 {len(df):,} 行")
    out_path = str(Path(output_dir) / "equity_dividend_history.parquet")
    write_parquet(df, output_dir, "equity_dividend_history.parquet")
    logger.info(f"equity_dividend_history 写入完成: {out_path} ({len(df):,} 行)")


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
        ("equity_structure", lambda: fetch_equity_structure(ido, code_list, output_dir)),
        ("equity_dividend",  lambda: fetch_equity_dividend(ido, code_list, output_dir)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下数据拉取失败: {errors}")
    logger.info("fetch_equity.py 全部完成")


if __name__ == "__main__":
    main()
