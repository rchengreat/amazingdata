"""
fetch_kline.py — 拉取每日行情数据（股票/指数/ETF）

输出文件：
  - extra_stock_{date}.parquet  ← query_kline(EXTRA_STOCK_A) + backward_factor
  - extra_index_{date}.parquet  ← query_kline(EXTRA_INDEX_A_SH_SZ)
  - extra_etf_{date}.parquet    ← query_kline(EXTRA_ETF)

日期逻辑：
  - 若指定 --date，直接拉该日期
  - 若不指定，使用今天日期（begin_date = end_date = today）
  - 每天的数据保存为独立的日期文件（monthly_cleanup.py 负责月末合并）
  - 若当日文件已存在则跳过

运行时间：工作日 15:15 / 15:45 / 16:00
"""
import os
import sys
import argparse
from datetime import date
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


def fetch_stock(bdo, mdo, trade_date: str, output_dir: str):
    out_path = Path(output_dir) / f"extra_stock_{trade_date}.parquet"
    if out_path.exists():
        logger.info(f"extra_stock_{trade_date}.parquet 已存在，跳过")
        return

    # 从 info_stock_factor.parquet 读取复权因子（由 fetch_stock_info.py 每日更新）
    factor_path = Path(output_dir) / "info_stock_factor.parquet"
    if not factor_path.exists():
        raise FileNotFoundError(
            f"info_stock_factor.parquet 不存在于 {output_dir}，请先运行 fetch_stock_info.py"
        )
    begin_idx = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    df_factor_full = pd.read_parquet(factor_path)
    df_factor = df_factor_full[df_factor_full["datetime"].astype(str) == begin_idx].copy()
    if df_factor.empty:
        logger.error(
            f"❌ 错误：info_stock_factor.parquet 中无 {begin_idx} 的复权因子数据，"
            f"无法生成 extra_stock_{trade_date}.parquet 文件。"
            f"请先运行 fetch_stock_info.py 或确认 {trade_date} 是交易日"
        )
        raise ValueError(
            f"info_stock_factor.parquet 中无 {begin_idx} 的复权因子数据，"
            f"无法生成文件"
        )
    logger.info(f"从 info_stock_factor.parquet 读取复权因子：{len(df_factor):,} 行（{begin_idx}）")
    df_factor.columns = ["code", "kline_time", "backward_factor"]
    df_factor["kline_time"] = pd.to_datetime(df_factor["kline_time"]).astype("datetime64[ns]")

    logger.info(f"拉取 extra_stock_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_STOCK_A")
    logger.info(f"股票代码数: {len(code_list)}")

    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_STOCK_A {trade_date} 返回空（非交易日？）")
        return
    df = dict_to_df(df_kline)
    logger.info(f"query_kline 返回 {len(df):,} 行")

    df = pd.merge(df, df_factor, on=["kline_time", "code"], how="left")
    if df["backward_factor"].isnull().all():
        logger.error(
            f"❌ 错误：复权因子合并后全为空，无法生成 extra_stock_{trade_date}.parquet 文件。"
            f"请检查 info_stock_factor.parquet 中 {begin_idx} 数据"
        )
        raise ValueError(
            f"复权因子合并后全为空，无法生成文件"
        )

    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_stock_{trade_date}.parquet")
    logger.info(f"extra_stock_{trade_date}.parquet 写入完成: {out_path} ({len(df):,} 行)")


def fetch_index(bdo, mdo, trade_date: str, output_dir: str):
    out_path = Path(output_dir) / f"extra_index_{trade_date}.parquet"
    if out_path.exists():
        logger.info(f"extra_index_{trade_date}.parquet 已存在，跳过")
        return

    logger.info(f"拉取 extra_index_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_INDEX_A_SH_SZ")
    logger.info(f"指数代码数: {len(code_list)}")
    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_INDEX_A_SH_SZ {trade_date} 返回空（非交易日？）")
        return
    df = dict_to_df(df_kline)
    logger.info(f"query_kline 返回 {len(df):,} 行")
    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_index_{trade_date}.parquet")
    logger.info(f"extra_index_{trade_date}.parquet 写入完成: {out_path} ({len(df):,} 行)")


def fetch_etf(bdo, mdo, trade_date: str, output_dir: str):
    out_path = Path(output_dir) / f"extra_etf_{trade_date}.parquet"
    if out_path.exists():
        logger.info(f"extra_etf_{trade_date}.parquet 已存在，跳过")
        return

    logger.info(f"拉取 extra_etf_{trade_date}...")
    code_list = bdo.get_code_list("EXTRA_ETF")
    logger.info(f"ETF 代码数: {len(code_list)}")
    df_kline = mdo.query_kline(
        code_list,
        begin_date=int(trade_date),
        end_date=int(trade_date),
        period=ad.constant.Period.day.value,
    )
    if not df_kline:
        logger.warning(f"query_kline EXTRA_ETF {trade_date} 返回空（非交易日？）")
        return
    df = dict_to_df(df_kline)
    logger.info(f"query_kline 返回 {len(df):,} 行")
    df = df.reset_index(drop=True)
    df = normalize_kline_dtypes(df)
    write_parquet(df, output_dir, f"extra_etf_{trade_date}.parquet")
    logger.info(f"extra_etf_{trade_date}.parquet 写入完成: {out_path} ({len(df):,} 行)")


def main():
    parser = argparse.ArgumentParser(description="拉取每日行情数据")
    parser.add_argument("--date", default=None, help="交易日期 YYYYMMDD，默认今天")
    parser.add_argument(
        "--type",
        choices=["stock", "index", "etf", "all"],
        default="all",
        help="拉取类型：stock / index / etf / all（默认 all）",
    )
    args = parser.parse_args()

    trade_date = args.date if args.date else date.today().strftime("%Y%m%d")
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"登录 AmazingData... 拉取日期: {trade_date}")
    get_client()

    bdo = ad.BaseData()
    calendar = bdo.get_calendar()
    mdo = ad.MarketData(calendar)

    ktypes = ["stock", "index", "etf"] if args.type == "all" else [args.type]

    for ktype in ktypes:
        if ktype == "stock":
            fetch_stock(bdo, mdo, trade_date, output_dir)
        elif ktype == "index":
            fetch_index(bdo, mdo, trade_date, output_dir)
        elif ktype == "etf":
            fetch_etf(bdo, mdo, trade_date, output_dir)

    logger.info("fetch_kline.py 全部完成")


if __name__ == "__main__":
    main()
