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
from amazingdata_fetcher.monitor import SystemMonitor

load_dotenv()

CST_OFFSET = dt.timedelta(hours=8)


def _now_cst():
    return dt.datetime.utcnow() + CST_OFFSET


def fetch_stock_basic(bdo, ido, output_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 info_stock_basic（增量：仅新上市代码）")
    out_path = str(Path(output_dir) / "info_stock_basic.parquet")
    existing = load_existing(out_path)

    mon.snapshot("stock_basic_start")
    logger.info("调用 bdo.get_code_list()...")
    all_codes = bdo.get_code_list()
    mon.snapshot("stock_basic_after_get_code_list")
    logger.info(f"全量代码数: {len(all_codes)}")

    delta_codes = new_codes(existing, all_codes, code_col="MARKET_CODE")
    if not delta_codes:
        logger.info("无新增代码，跳过 get_stock_basic")
        return

    logger.info(f"新增代码数: {len(delta_codes)}，开始拉取...")
    mon.snapshot("stock_basic_before_get_stock_basic")
    df_new = ido.get_stock_basic(delta_codes)
    mon.snapshot("stock_basic_after_get_stock_basic")
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


def fetch_stock_factor(bdo, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    """
    与 extract_ad_stock.ipynb 使用完全相同的逻辑：
      1. get_code_list('EXTRA_STOCK_A')
      2. get_backward_factor(code_list, local_path=tmp_cache, is_local=False)
      3. unstack().reset_index()，columns = ['instrument', 'datetime', 'backward_factor']
      4. 全量覆写（zstd 压缩）

    跳过条件：文件已在今日 15:30 CST 后写入（当日收盘后已是最新）。
    Docker 容器时区为 UTC，需手动偏移 +8 小时。
    """
    logger.info("=" * 60)
    logger.info("开始拉取 info_stock_factor（全量覆写，get_backward_factor）")
    out_path = Path(output_dir) / "info_stock_factor.parquet"

    if out_path.exists():
        mtime = dt.datetime.fromtimestamp(out_path.stat().st_mtime)
        now_cst = _now_cst()
        market_close_today = dt.datetime.combine(now_cst.date(), dt.time(15, 30))
        if mtime >= market_close_today:
            logger.info(
                f"info_stock_factor 已在今日 {mtime.strftime('%H:%M')} 收盘后写入，跳过下载"
            )
            return

    mon.snapshot("stock_factor_before_get_code_list")
    logger.info("调用 bdo.get_code_list('EXTRA_STOCK_A')...")
    code_list = bdo.get_code_list("EXTRA_STOCK_A")
    mon.snapshot("stock_factor_after_get_code_list")
    logger.info(f"股票代码数: {len(code_list)}，开始下载复权因子（全量，约 4500 万行）...")

    tmp_cache = "/tmp/stock_factor_cache/"
    Path(tmp_cache).mkdir(parents=True, exist_ok=True)
    mon.snapshot("stock_factor_before_get_backward_factor")
    logger.info("调用 bdo.get_backward_factor()...")
    df_factor = bdo.get_backward_factor(code_list, local_path=tmp_cache, is_local=False)
    mon.snapshot("stock_factor_after_get_backward_factor")
    if df_factor is None or df_factor.empty:
        logger.error("get_backward_factor 返回空数据")
        return

    logger.info(f"原始宽表: {df_factor.shape}，开始 unstack 转换为长表...")

    df_factor = df_factor.unstack().reset_index()
    df_factor.columns = ["instrument", "datetime", "backward_factor"]
    mon.snapshot("stock_factor_after_unstack")

    logger.info(
        f"长表行数: {len(df_factor)}，"
        f"日期范围: {df_factor['datetime'].min()} ~ {df_factor['datetime'].max()}"
    )

    mon.snapshot("stock_factor_before_get_calendar")
    logger.info("调用 bdo.get_calendar() 校验日期...")
    calendar = bdo.get_calendar()
    mon.snapshot("stock_factor_after_get_calendar")
    today_str = str(calendar[-1])
    max_dt_str = pd.to_datetime(df_factor["datetime"].max()).strftime("%Y%m%d")
    if max_dt_str != today_str:
        logger.warning(f"复权因子最新日期 {max_dt_str} != 交易日历最新 {today_str}，请检查")
    else:
        logger.info(f"复权因子日期校验通过: {max_dt_str}")

    write_parquet(df_factor, output_dir, "info_stock_factor.parquet")
    mon.snapshot("stock_factor_done")
    logger.info("info_stock_factor 写入完成")


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    sdk_cache_dir = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache")
    Path(sdk_cache_dir).mkdir(parents=True, exist_ok=True)

    mon = SystemMonitor()
    mon.snapshot("startup")

    logger.info("登录 AmazingData...")
    get_client()
    mon.snapshot("after_login")

    bdo = ad.BaseData()
    ido = ad.InfoData()

    errors = []
    for name, fn in [
        ("stock_basic", lambda: fetch_stock_basic(bdo, ido, output_dir, mon)),
        ("stock_factor", lambda: fetch_stock_factor(bdo, output_dir, sdk_cache_dir, mon)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            mon.snapshot(f"{name}_error")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下数据拉取失败: {errors}")
    logger.info("fetch_stock_info.py 全部完成")
    os._exit(0)


if __name__ == "__main__":
    main()
