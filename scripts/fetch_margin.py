"""
fetch_margin.py — 拉取融资融券数据（增量模式）

包含：
  - margin_summary_history.parquet  ← get_margin_summary（追加新 TRADE_DATE 行）
  - margin_detail_history.parquet   ← get_margin_detail（追加新 TRADE_DATE 行）

增量策略：
  SDK 不支持日期过滤，每次全量下载后客户端过滤。
  过滤基准：已有文件中 TRADE_DATE 的最大值，只追加新日期行。

运行时间：工作日 16:15 / 16:30
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
from amazingdata_fetcher.monitor import SystemMonitor

load_dotenv()


def dict_to_df(d: dict) -> pd.DataFrame:
    non_empty = [v for v in d.values() if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _sdk_fetch(fn, *args):
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def fetch_margin_summary(ido, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_summary_history（增量：追加新 TRADE_DATE 行）")
    out_path = str(Path(output_dir) / "margin_summary_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

    tmp_cache = sdk_cache_dir
    mon.snapshot("margin_summary_before_sdk")
    df = _sdk_fetch(ido.get_margin_summary, tmp_cache, False)
    mon.snapshot("margin_summary_after_sdk")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_margin_summary 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    if result is existing:
        logger.info("无新增行，跳过写入")
        return
    write_parquet(result, output_dir, "margin_summary_history.parquet")
    logger.info("margin_summary_history 写入完成")


def fetch_margin_detail(ido, code_list: list, output_dir: str, sdk_cache_dir: str, mon: SystemMonitor):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_detail_history（增量：追加新 TRADE_DATE 行）")
    out_path = Path(output_dir) / "margin_detail_history.parquet"

    existing_max_dt = None
    if out_path.exists() and out_path.stat().st_size > 10_000:
        existing_max_dt = max_date_str(
            pd.read_parquet(out_path, columns=["TRADE_DATE"]), "TRADE_DATE"
        )
        logger.info(f"已有文件最大 TRADE_DATE: {existing_max_dt}")
    else:
        logger.info("无已有文件，全量写入")

    tmp_cache = sdk_cache_dir
    mon.snapshot("margin_detail_before_sdk")
    df = _sdk_fetch(ido.get_margin_detail, code_list, tmp_cache, False)
    mon.snapshot("margin_detail_after_sdk")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.warning("get_margin_detail 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)
    logger.info(f"SDK 返回 {len(df):,} 行")

    if existing_max_dt is None:
        result = df.reset_index(drop=True)
    else:
        col = df["TRADE_DATE"]
        if pd.api.types.is_datetime64_any_dtype(col):
            mask = col.dt.strftime("%Y%m%d") > existing_max_dt
        else:
            mask = col.astype(str).str[:8] > existing_max_dt
        new_rows = df[mask]
        del df
        logger.info(f"增量行数: {len(new_rows):,}（已有最大日期: {existing_max_dt}）")
        if new_rows.empty:
            logger.info("无新增行，跳过写入")
            return
        existing = pd.read_parquet(out_path)
        result = pd.concat([existing, new_rows], ignore_index=True)
        del existing, new_rows

    write_parquet(result, str(out_path.parent), out_path.name)
    logger.info("margin_detail_history 写入完成")


def load_exclusion_list(output_dir: str) -> set:
    csv_path = Path(output_dir) / "no_margin_stock_list.csv"
    if not csv_path.exists():
        logger.warning(f"排除清单文件不存在: {csv_path}，跳过过滤")
        return set()
    df = pd.read_csv(csv_path, dtype=str)
    if "stock_code" not in df.columns:
        logger.warning(f"排除清单文件缺少 stock_code 列: {csv_path}，跳过过滤")
        return set()
    codes = set(df["stock_code"].str.strip())
    logger.info(f"排除清单: {len(codes)} 只股票（{csv_path}）")
    return codes


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

    code_list = bdo.get_code_list()
    logger.info(f"获取到 {len(code_list)} 个股票代码")

    exclusion = load_exclusion_list(output_dir)
    if exclusion:
        code_list = [c for c in code_list if c not in exclusion]
        logger.info(f"排除 {len(exclusion)} 只后，剩余 {len(code_list)} 只股票代码")

    errors = []
    for name, fn in [
        ("margin_summary", lambda: fetch_margin_summary(ido, output_dir, sdk_cache_dir, mon)),
        ("margin_detail",  lambda: fetch_margin_detail(ido, code_list, output_dir, sdk_cache_dir, mon)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            mon.snapshot(f"{name}_error")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下数据拉取失败: {errors}")
    logger.info("fetch_margin.py 全部完成")
    os._exit(0)


if __name__ == "__main__":
    main()
