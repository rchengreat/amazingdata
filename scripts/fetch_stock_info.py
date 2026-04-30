"""
fetch_stock_info.py — 拉取股票基础信息与复权因子

输出文件：
  - info_stock_basic.parquet    ← ido.get_stock_basic（增量：仅新上市代码）
  - info_stock_factor.parquet   ← bdo.get_backward_factor（全量覆写，宽表→长表）

复权因子说明：
  每次全量下载（~4500 万行宽表），unstack 为长表后全量覆写。
  不做增量追加——复权因子会对历史日期溯源调整，必须每次覆写。
  若文件已在今日收盘后（15:30）写入，则跳过下载。

运行时间：工作日 03:00
"""
import datetime as dt
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
from amazingdata_fetcher.incremental import load_existing, new_codes

load_dotenv()


def fetch_stock_basic(bdo, ido, output_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 info_stock_basic（增量：仅新上市代码）")
    out_path = str(Path(output_dir) / "info_stock_basic.parquet")
    existing = load_existing(out_path)

    all_codes = bdo.get_code_list()
    logger.info(f"全量代码数: {len(all_codes)}")

    delta_codes = new_codes(existing, all_codes, code_col="MARKET_CODE")
    if not delta_codes:
        logger.info("无新增代码，跳过 get_stock_basic")
        return

    logger.info(f"新增代码数: {len(delta_codes)}，开始拉取...")
    df_new = ido.get_stock_basic(delta_codes)
    if df_new is None or (isinstance(df_new, pd.DataFrame) and df_new.empty):
        logger.warning("get_stock_basic 返回空数据")
        return
    if isinstance(df_new, dict):
        df_new = pd.concat(list(df_new.values()), ignore_index=True)
    df_new = df_new.reset_index(drop=True)
    logger.info(f"拉取到 {len(df_new)} 行新数据")

    if existing is not None:
        result = pd.concat([existing, df_new], ignore_index=True)
    else:
        result = df_new

    write_parquet(result, output_dir, "info_stock_basic.parquet")
    logger.info("info_stock_basic 写入完成")


def fetch_stock_factor(bdo, output_dir: str, sdk_cache_dir: str):
    """
    与 extract_ad_stock.ipynb 使用完全相同的逻辑：
      1. get_code_list('EXTRA_STOCK_A')
      2. get_backward_factor(code_list, local_path=sdk_cache_dir, is_local=False)
      3. unstack().reset_index()，columns = ['instrument', 'datetime', 'backward_factor']
      4. 全量覆写（zstd 压缩）

    跳过条件：文件已在今日 15:30 后写入（当日收盘后已是最新）。
    """
    logger.info("=" * 60)
    logger.info("开始拉取 info_stock_factor（全量覆写，get_backward_factor）")
    out_path = Path(output_dir) / "info_stock_factor.parquet"

    if out_path.exists():
        mtime = dt.datetime.fromtimestamp(out_path.stat().st_mtime)
        market_close_today = dt.datetime.combine(dt.date.today(), dt.time(15, 30))
        if mtime >= market_close_today:
            logger.info(
                f"info_stock_factor 已在今日 {mtime.strftime('%H:%M')} 收盘后写入，跳过下载"
            )
            return

    # 与 notebook 完全一致：传入 output_dir 作为 local_path
    code_list = bdo.get_code_list("EXTRA_STOCK_A")
    logger.info(f"股票代码数: {len(code_list)}，开始下载复权因子（全量，约 4500 万行）...")

    df_factor = bdo.get_backward_factor(code_list, local_path=sdk_cache_dir, is_local=False)
    if df_factor is None or df_factor.empty:
        logger.error("get_backward_factor 返回空数据")
        return

    logger.info(f"原始宽表: {df_factor.shape}，开始 unstack 转换为长表...")

    # 与 notebook 完全一致的转换逻辑
    df_factor = df_factor.unstack().reset_index()
    df_factor.columns = ["instrument", "datetime", "backward_factor"]

    logger.info(
        f"长表行数: {len(df_factor)}，"
        f"日期范围: {df_factor['datetime'].min()} ~ {df_factor['datetime'].max()}"
    )

    # 校验最新日期（参考 notebook 的校验逻辑）
    calendar = bdo.get_calendar()
    today_str = str(calendar[-1])
    max_dt_str = pd.to_datetime(df_factor["datetime"].max()).strftime("%Y%m%d")
    if max_dt_str != today_str:
        logger.warning(f"复权因子最新日期 {max_dt_str} != 交易日历最新 {today_str}，请检查")
    else:
        logger.info(f"复权因子日期校验通过: {max_dt_str}")

    write_parquet(df_factor, output_dir, "info_stock_factor.parquet")
    logger.info("info_stock_factor 写入完成")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info("登录 AmazingData...")
    get_client()

    bdo = ad.BaseData()
    ido = ad.InfoData()

    fetch_stock_basic(bdo, ido, output_dir)
    fetch_stock_factor(bdo, output_dir, sdk_cache_dir)

    logger.info("fetch_stock_info.py 全部完成")


if __name__ == "__main__":
    main()
