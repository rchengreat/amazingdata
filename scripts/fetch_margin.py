"""
fetch_margin.py — 拉取融资融券数据（增量模式）

包含：
  - margin_summary_history.parquet  ← get_margin_summary（追加新 TRADE_DATE 行）
  - margin_detail_history.parquet   ← get_margin_detail（追加新 TRADE_DATE 行）

margin_detail 策略：
  - 已有 SDK 缓存的股票：is_local=True（直接读本地 HDF5，无网络，内存极低）
  - 未缓存的股票：is_local=False，分批下载（每批 BATCH_SIZE 只），控制峰值内存
  - 所有批次完成后，与已有 parquet 合并，只追加新日期行

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

load_dotenv()

BATCH_SIZE = 200  # stocks per remote fetch batch


def dict_to_df(d: dict) -> pd.DataFrame:
    non_empty = [v for v in d.values() if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def _sdk_fetch(fn, *args):
    """Wrap SDK call to catch HDF5/tables cache errors without aborting."""
    try:
        return fn(*args)
    except Exception as e:
        logger.warning(f"SDK 调用异常（已跳过）: {type(e).__name__}: {e}")
        return None


def fetch_margin_summary(ido, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_summary_history（增量：追加新 TRADE_DATE 行）")
    out_path = str(Path(output_dir) / "margin_summary_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "TRADE_DATE") if existing is not None else None

    df = _sdk_fetch(ido.get_margin_summary, sdk_cache_dir, False)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        logger.error("get_margin_summary 返回空数据，跳过")
        return
    if isinstance(df, dict):
        df = dict_to_df(df)

    result = append_new_rows(existing, df, "TRADE_DATE", max_dt)
    write_parquet(result, output_dir, "margin_summary_history.parquet")
    logger.info("margin_summary_history 写入完成")


def fetch_margin_detail(ido, code_list: list, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 margin_detail_history（增量：追加新 TRADE_DATE 行）")
    out_path = Path(output_dir) / "margin_detail_history.parquet"

    # Read only TRADE_DATE column to get max date — avoid loading 5M+ rows
    existing_max_dt = None
    if out_path.exists() and out_path.stat().st_size > 10_000:
        existing_max_dt = max_date_str(
            pd.read_parquet(out_path, columns=["TRADE_DATE"]), "TRADE_DATE"
        )
        logger.info(f"已有文件最大 TRADE_DATE: {existing_max_dt}")
    else:
        logger.info("无已有文件，全量写入")

    # Split codes into cached (is_local=True) vs uncached (is_local=False)
    cache_detail_dir = Path(sdk_cache_dir) / "infodata" / "margin_detail"
    cached_codes = set()
    if cache_detail_dir.exists():
        cached_codes = {f.stem for f in cache_detail_dir.glob("*.h5")}
    logger.info(f"SDK 缓存中已有 {len(cached_codes)} 只股票，共 {len(code_list)} 只")

    local_codes = [c for c in code_list if c in cached_codes]
    remote_codes = [c for c in code_list if c not in cached_codes]
    logger.info(f"本地读取: {len(local_codes)} 只，远程下载: {len(remote_codes)} 只（每批 {BATCH_SIZE} 只）")

    all_chunks = []

    # --- Batch 0: read all cached stocks at once (is_local=True, no network) ---
    if local_codes:
        logger.info(f"读取本地缓存 {len(local_codes)} 只...")
        result = _sdk_fetch(ido.get_margin_detail, local_codes, sdk_cache_dir, True)
        if result is not None:
            chunk = dict_to_df(result) if isinstance(result, dict) else result
            if not chunk.empty:
                all_chunks.append(chunk)
                logger.info(f"本地缓存读取完成，{len(chunk):,} 行")
        del result

    # --- Remote batches: download + cache (is_local=False) ---
    n_batches = (len(remote_codes) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(n_batches):
        batch = remote_codes[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        logger.info(f"远程批次 {i+1}/{n_batches}：下载 {len(batch)} 只...")
        result = _sdk_fetch(ido.get_margin_detail, batch, sdk_cache_dir, False)
        if result is not None:
            chunk = dict_to_df(result) if isinstance(result, dict) else result
            if not chunk.empty:
                all_chunks.append(chunk)
                logger.info(f"批次 {i+1} 完成，{len(chunk):,} 行")
        del result

    if not all_chunks:
        logger.error("所有批次均未返回数据，跳过写入")
        return

    logger.info(f"合并 {len(all_chunks)} 个批次...")
    df = pd.concat(all_chunks, ignore_index=True)
    del all_chunks
    logger.info(f"SDK 合计返回 {len(df):,} 行")

    # Filter to new rows only, then concat with existing
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
        ("margin_summary", lambda: fetch_margin_summary(ido, output_dir, sdk_cache_dir)),
        ("margin_detail",  lambda: fetch_margin_detail(ido, code_list, output_dir, sdk_cache_dir)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"fetch_{name} 失败: {type(e).__name__}: {e}")
            errors.append(name)

    if errors:
        raise RuntimeError(f"以下数据拉取失败: {errors}")
    logger.info("fetch_margin.py 全部完成")


if __name__ == "__main__":
    main()
