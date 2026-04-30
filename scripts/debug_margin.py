"""
Temporary debug script — inspect what get_margin_detail actually returns.
Run once manually, then delete.
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import AmazingData as ad
from amazingdata_fetcher.client import get_client

load_dotenv()

get_client()
bdo = ad.BaseData()
ido = ad.InfoData()

sdk_cache = os.environ.get("SDK_CACHE_DIR", "/volume1/amazingdata/sdk_cache/")
code_list = bdo.get_code_list()
logger.info(f"Total codes: {len(code_list)}, testing with first 3")

result = ido.get_margin_detail(code_list[:3], local_path=sdk_cache, is_local=False)

logger.info(f"Return type: {type(result)}")

if result is None:
    logger.error("RESULT IS NONE")
elif isinstance(result, dict):
    logger.info(f"Dict with {len(result)} keys, sample keys: {list(result.keys())[:5]}")
    non_empty = {k: v for k, v in result.items() if v is not None and not v.empty}
    logger.info(f"Non-empty values: {len(non_empty)} / {len(result)}")
    if non_empty:
        sample_df = list(non_empty.values())[0]
        logger.info(f"Sample df shape: {sample_df.shape}, cols: {list(sample_df.columns)}")
    else:
        logger.error("ALL VALUES ARE EMPTY OR NONE")
elif isinstance(result, pd.DataFrame):
    logger.info(f"DataFrame shape: {result.shape}")
    if result.empty:
        logger.error("DATAFRAME IS EMPTY")
    else:
        logger.info(f"Columns: {list(result.columns)}")
        logger.info(f"Sample:\n{result.head(2)}")
else:
    logger.info(f"Unexpected type: {type(result)}, value: {result}")
