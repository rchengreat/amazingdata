"""
fetch_info.py — 拉取证券基础信息类数据（增量模式）

包含：
  - info_stock_basic.parquet                ← get_code_info + get_stock_basic（仅新代码）
  - info_stock_factor.parquet               ← get_backward_factor（追加新日期行）
  - info_industry_basic_history.parquet     ← get_industry_base_info（全量刷新，小文件）
  - info_industry_detail_history.parquet    ← get_industry_constituent（追加新 INDATE 行）
  - info_index_detail_history.parquet       ← get_index_constituent（追加新 INDATE 行）
  - info_index_weight_history.parquet       ← get_index_weight（追加新 TRADE_DATE 行）

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
from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows, new_codes

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    return pd.concat(list(d.values()), ignore_index=True)


def fetch_stock_basic(bdo, ido, output_dir: str):
    logger.info("拉取 info_stock_basic（增量：仅新上市代码）...")
    out_path = str(Path(output_dir) / "info_stock_basic.parquet")
    existing = load_existing(out_path)

    # get_code_info 返回每日最新证券信息，速度很快
    df_code_info = bdo.get_code_info()
    if df_code_info is None or (isinstance(df_code_info, pd.DataFrame) and df_code_info.empty):
        logger.error("get_code_info 返回空数据，跳过")
        return

    if isinstance(df_code_info, dict):
        df_code_info = dict_to_df(df_code_info)

    # 只取上海/深圳 A 股（与 get_code_list() 范围一致）
    all_codes = bdo.get_code_list()
    delta_codes = new_codes(existing, all_codes, code_col="MARKET_CODE")

    if not delta_codes:
        logger.info("无新增代码，跳过 get_stock_basic")
        return

    df_new = ido.get_stock_basic(delta_codes)
    if df_new is None or (isinstance(df_new, pd.DataFrame) and df_new.empty):
        logger.warning("get_stock_basic 返回空数据")
        return
    if isinstance(df_new, dict):
        df_new = dict_to_df(df_new)
    df_new = df_new.reset_index(drop=True)

    if existing is not None:
        result = pd.concat([existing, df_new], ignore_index=True)
    else:
        result = df_new

    write_parquet(result, output_dir, "info_stock_basic.parquet")


def fetch_stock_factor(bdo, output_dir: str, sdk_cache_dir: str):
    """
    get_backward_factor 每次下载全量宽表（~4500 万行），耗时长。
    策略：若现有数据不超过 7 天旧，直接跳过，避免每天重复下载。
    """
    from datetime import date, timedelta
    out_path = str(Path(output_dir) / "info_stock_factor.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "datetime") if existing is not None else None

    if max_dt is not None:
        days_old = (date.today() - date.fromisoformat(f"{max_dt[:4]}-{max_dt[4:6]}-{max_dt[6:]}")).days
        if days_old <= 7:
            logger.info(f"info_stock_factor 已是 {days_old} 天前数据（{max_dt}），跳过下载")
            return

    logger.info("拉取 info_stock_factor（全量下载，get_backward_factor）...")
    code_list = bdo.get_code_list()
    df_factor = bdo.get_backward_factor(code_list, sdk_cache_dir, is_local=False)
    if df_factor is None or df_factor.empty:
        logger.error("get_backward_factor 返回空数据")
        return

    # 宽表 → 长表
    df_long = df_factor.unstack().reset_index()
    df_long.columns = ["instrument", "datetime", "backward_factor"]
    df_long["datetime"] = pd.to_datetime(df_long["datetime"])

    result = append_new_rows(existing, df_long, "datetime", max_dt)
    write_parquet(result, output_dir, "info_stock_factor.parquet")


def fetch_industry_basic(ido, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 info_industry_basic_history（全量刷新，小文件）...")
    df = ido.get_industry_base_info(local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_industry_base_info 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)
    df = df.reset_index(drop=True)
    write_parquet(df, output_dir, "info_industry_basic_history.parquet")


def fetch_industry_detail(ido, industry_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 info_industry_detail_history（增量：追加新 INDATE 行）...")
    out_path = str(Path(output_dir) / "info_industry_detail_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "INDATE") if existing is not None else None

    df = ido.get_industry_constituent(industry_codes, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_industry_constituent 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "INDATE", max_dt)
    write_parquet(result, output_dir, "info_industry_detail_history.parquet")


def fetch_index_detail(ido, index_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 info_index_detail_history（增量：追加新 INDATE 行）...")
    out_path = str(Path(output_dir) / "info_index_detail_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "INDATE") if existing is not None else None

    df = ido.get_index_constituent(index_codes, local_path=sdk_cache_dir, is_local=False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_index_constituent 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "INDATE", max_dt)
    write_parquet(result, output_dir, "info_index_detail_history.parquet")


def fetch_index_weight(ido, index_codes: list, output_dir: str, sdk_cache_dir: str):
    logger.info("拉取 info_index_weight_history（增量：追加新 TRADE_DATE 行）...")
    out_path = str(Path(output_dir) / "info_index_weight_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

    try:
        df = ido.get_index_weight(index_codes, local_path=sdk_cache_dir, is_local=False)
    except Exception as e:
        logger.error(f"get_index_weight SDK 异常: {e}，保留已有文件不覆盖")
        return

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_index_weight 返回空数据")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    write_parquet(result, output_dir, "info_index_weight_history.parquet")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    ido = ad.InfoData()

    # 指数代码：沪深交易所指数（~624 个）
    index_codes = bdo.get_code_list("EXTRA_INDEX_A_SH_SZ")
    logger.info(f"获取到 {len(index_codes)} 个指数代码")

    fetch_stock_basic(bdo, ido, output_dir)
    fetch_stock_factor(bdo, output_dir, sdk_cache_dir)
    fetch_industry_basic(ido, output_dir, sdk_cache_dir)

    # 从已写入的 industry_basic 读取行业指数代码
    industry_basic_path = Path(output_dir) / "info_industry_basic_history.parquet"
    if industry_basic_path.exists():
        df_ind = pd.read_parquet(industry_basic_path)
        industry_codes = df_ind["INDEX_CODE"].dropna().unique().tolist()
        logger.info(f"获取到 {len(industry_codes)} 个行业指数代码")
    else:
        logger.warning("info_industry_basic_history.parquet 不存在，跳过行业成分拉取")
        industry_codes = []

    if industry_codes:
        fetch_industry_detail(ido, industry_codes, output_dir, sdk_cache_dir)

    fetch_index_detail(ido, index_codes, output_dir, sdk_cache_dir)
    fetch_index_weight(ido, index_codes, output_dir, sdk_cache_dir)

    logger.info("fetch_info.py 全部完成")


if __name__ == "__main__":
    main()
