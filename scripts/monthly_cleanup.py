"""
monthly_cleanup.py — 每月初合并上月增量 K 线文件，删除日文件

每月2日运行（月度任务），将上月所有每日增量文件合并为历史汇总文件：
  - extra_stock_history.parquet
  - extra_index_history.parquet
  - extra_etf_history.parquet

合并后删除上月的日文件（extra_{type}_YYYYMM??.parquet）。

用法：
    python scripts/monthly_cleanup.py               # 处理上个月
    python scripts/monthly_cleanup.py --month 202603  # 处理指定年月
"""
import os
import sys
import argparse
import glob
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
from amazingdata_fetcher.writer import write_parquet

load_dotenv()


def get_last_month(ref_date: date) -> str:
    """返回上个月的 YYYYMM 字符串"""
    first_of_month = ref_date.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return last_month.strftime("%Y%m")


def merge_and_cleanup(data_type: str, month_str: str, output_dir: str):
    """
    合并指定月份的增量文件为历史汇总文件，然后删除日文件。
    data_type: 'stock' | 'index' | 'etf'
    month_str: 'YYYYMM'
    """
    pattern = os.path.join(output_dir, f"extra_{data_type}_{month_str}??.parquet")
    daily_files = sorted(glob.glob(pattern))

    if not daily_files:
        logger.warning(f"未找到 {pattern} 匹配文件，跳过")
        return

    logger.info(f"找到 {len(daily_files)} 个 extra_{data_type}_{month_str}?? 文件，开始合并...")

    history_file = os.path.join(output_dir, f"extra_{data_type}_history.parquet")

    # 读取当月所有日文件
    dfs = [pd.read_parquet(f) for f in daily_files]
    df_new = pd.concat(dfs, ignore_index=True)
    logger.info(f"当月新增数据：{len(df_new)} 行")

    # 若历史文件存在，追加
    if os.path.exists(history_file):
        df_history = pd.read_parquet(history_file)
        logger.info(f"历史文件已有 {len(df_history)} 行，合并中...")
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
    else:
        df_combined = df_new

    write_parquet(df_combined, output_dir, f"extra_{data_type}_history.parquet")
    logger.info(f"历史文件已写入：{history_file}（共 {len(df_combined)} 行）")

    # 删除当月日文件
    for f in daily_files:
        os.remove(f)
        logger.info(f"已删除：{f}")


def main():
    parser = argparse.ArgumentParser(description="合并上月增量 K 线文件")
    parser.add_argument(
        "--month",
        default=None,
        help="要处理的年月 YYYYMM，默认为上个月",
    )
    args = parser.parse_args()

    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")

    if args.month:
        month_str = args.month
    else:
        month_str = get_last_month(date.today())

    logger.info(f"处理月份：{month_str}，数据目录：{output_dir}")

    for data_type in ("stock", "index", "etf"):
        merge_and_cleanup(data_type, month_str, output_dir)

    logger.info("monthly_cleanup.py 完成")


if __name__ == "__main__":
    main()
