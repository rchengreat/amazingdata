"""
fetch_code_info.py — 拉取每日最新证券信息（3.5.2.1）

对应 SDK 接口：BaseData.get_code_info(security_type)
输出字段：stock_code(index), symbol, security_status, pre_close,
          high_limited, low_limited, price_tick

用法：
    python scripts/fetch_code_info.py
"""
import os
import sys
from datetime import date
from pathlib import Path

# 允许从项目根目录直接运行
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from loguru import logger
import AmazingData as ad

from amazingdata_fetcher.client import get_client
from amazingdata_fetcher.writer import write_parquet

load_dotenv()


def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/volume1/amazingdata/data")
    today = date.today().strftime("%Y%m%d")

    logger.info("登录 AmazingData...")
    get_client()

    logger.info("拉取每日最新证券信息（沪深北 A 股）...")
    base = ad.BaseData()
    code_info = base.get_code_info(security_type="EXTRA_STOCK_A")

    if code_info is None or code_info.empty:
        logger.error("返回数据为空，请检查账号权限或网络连接")
        sys.exit(1)

    logger.info(f"获取到 {len(code_info)} 条记录，字段：{list(code_info.columns)}")

    filename = f"code_info_{today}.parquet"
    write_parquet(code_info.reset_index(), output_dir, filename)

    logger.info("完成")


if __name__ == "__main__":
    main()
