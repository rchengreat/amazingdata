"""
fetch_kline.py — 拉取每日增量行情数据（股票/指数/ETF）

输出文件：
  - extra_stock_{date}.parquet  ← query_kline(EXTRA_STOCK_A_SH_SZ) + backward_factor
  - extra_index_{date}.parquet  ← query_kline(EXTRA_INDEX_A_SH_SZ)
  - extra_etf_{date}.parquet    ← query_kline(EXTRA_ETF)

增量模式：
  - 若指定 --date，直接拉该日期
  - 若不指定，查找 extra_{type}_history.parquet 中最新 kline_time，
    然后拉 max_date+1 到今天的所有交易日（补漏）
  - 每天的数据保存为独立的日期文件（monthly_cleanup.py 负责月末合并）

运行时间：工作日 15:15 / 15:45 / 16:00
"""
import os
import sys
import argparse
from datetime import date, timedelta, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import AmazingData as ad

from amazingdata_fetcher.client import get_client
from amazingdata_fetcher.writer import write_parquet
from amazingdata_fetcher.incremental import load_existing, max_date_str

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    return pd.concat(list(d.values()), ignore_index=True)


def normalize_kline_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """统一 kline DataFrame 的类型，与 tgw 输出保持一致：
    - kline_time: datetime64[ns]（tgw 用 ns，SDK 默认返回 us）
    - amount: int64（tgw 截断为整数，SDK 返回浮点）
    """
    if pd.api.types.is_datetime64_any_dtype(df["kline_time"]):
        df["kline_time"] = df["kline_time"].astype("datetime64[ns]")
    df["amount"] = df["amount"].astype("int64")
    return df


def get_trade_dates_since(bdo, since_date: str, until_date: str) -> list[str]:
    """
    返回 [since_date, until_date] 之间的交易日列表（YYYYMMDD 字符串）。
    """
    calendar = bdo.get_calendar()
    # calendar 是 list/array of date strings or datetime-like
    result = []
    for d in calendar:
        d_str = str(d)[:10].replace("-", "")  # 统一为 YYYYMMDD
        if since_date <= d_str <= until_date:
            result.append(d_str)
    return sorted(result)


def pending_dates(bdo, ktype: str, output_dir: str, until_date: str) -> list[str]:
    """
    根据 extra_{ktype}_history.parquet 的最新日期和已有日期文件，
    计算出需要补拉的交易日列表。
    """
    # 先看 history 文件的最新日期
    history_path = str(Path(output_dir) / f"extra_{ktype}_history.parquet")
    existing = load_existing(history_path)
    max_dt = max_date_str(existing, "kline_time") if existing is not None else None

    if max_dt is None:
        # 没有历史文件，只拉 until_date 当天
        logger.info(f"无历史文件，仅拉取 {until_date}")
        return [until_date]

    # since = max_dt 的下一个交易日（不含 max_dt 当天，已有）
    # 先找出 max_dt+1 到 until_date 的交易日
    next_day = (datetime.strptime(max_dt, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    if next_day > until_date:
        logger.info(f"历史已是最新（{max_dt}），无需拉取")
        return []

    all_pending = get_trade_dates_since(bdo, next_day, until_date)

    # 排除已经存在的日期文件（可能上次漏合并的）
    missing = []
    for d in all_pending:
        daily_path = Path(output_dir) / f"extra_{ktype}_{d}.parquet"
        if not daily_path.exists():
            missing.append(d)

    logger.info(f"需要补拉的交易日: {missing}")
    return missing


def fetch_stock(bdo, mdo, trade_date: str, output_dir: str, sdk_cache_dir: str):
    logger.info(f"拉取 extra_stock_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_STOCK_A_SH_SZ")
    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_STOCK_A_SH_SZ {trade_date} 返回空（非交易日？）")
        return
    df = dict_to_df(df_kline)

    # 合并后复权因子：读取 fetch_info.py 已写好的长表，按日期过滤后双键合并
    date_str = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    factor_path = Path(output_dir) / "info_stock_factor.parquet"
    if factor_path.exists():
        df_factor = pd.read_parquet(factor_path)
        df_factor = df_factor[df_factor["datetime"] == date_str].copy()
        df_factor.columns = ["code", "kline_time", "backward_factor"]
        df = pd.merge(df, df_factor, on=["kline_time", "code"], how="left")
        if df["backward_factor"].isnull().all():
            logger.warning(f"info_stock_factor.parquet 中无 {date_str} 数据，backward_factor 留空")
    else:
        logger.warning(f"找不到 {factor_path}，backward_factor 留空")
        df["backward_factor"] = float("nan")

    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_stock_{trade_date}.parquet")


def fetch_index(bdo, mdo, trade_date: str, output_dir: str):
    logger.info(f"拉取 extra_index_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_INDEX_A_SH_SZ")
    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_INDEX_A_SH_SZ {trade_date} 返回空")
        return
    df = dict_to_df(df_kline)
    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_index_{trade_date}.parquet")


def fetch_etf(bdo, mdo, trade_date: str, output_dir: str):
    logger.info(f"拉取 extra_etf_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_ETF")
    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_ETF {trade_date} 返回空")
        return
    df = dict_to_df(df_kline)
    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_etf_{trade_date}.parquet")


def main():
    parser = argparse.ArgumentParser(description="拉取每日增量行情数据")
    parser.add_argument("--date", default=None, help="交易日期 YYYYMMDD，默认自动检测增量")
    parser.add_argument(
        "--type",
        choices=["stock", "index", "etf", "all"],
        default="all",
        help="拉取类型：stock / index / etf / all（默认 all）",
    )
    args = parser.parse_args()

    today = date.today().strftime("%Y%m%d")
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    calendar = bdo.get_calendar()
    mdo = ad.MarketData(calendar)

    ktypes = ["stock", "index", "etf"] if args.type == "all" else [args.type]

    for ktype in ktypes:
        if args.date:
            dates_to_fetch = [args.date]
        else:
            dates_to_fetch = pending_dates(bdo, ktype, output_dir, today)

        if not dates_to_fetch:
            logger.info(f"{ktype}: 已是最新，跳过")
            continue

        for trade_date in dates_to_fetch:
            if ktype == "stock":
                fetch_stock(bdo, mdo, trade_date, output_dir, sdk_cache_dir)
            elif ktype == "index":
                fetch_index(bdo, mdo, trade_date, output_dir)
            elif ktype == "etf":
                fetch_etf(bdo, mdo, trade_date, output_dir)

    logger.info("fetch_kline.py 全部完成")


if __name__ == "__main__":
    main()
