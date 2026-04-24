import os
import pandas as pd
from pathlib import Path
from loguru import logger


def write_parquet(df: pd.DataFrame, output_dir: str, filename: str) -> str:
    """将 DataFrame 写为 Parquet 文件，返回完整文件路径"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    df.to_parquet(filepath, index=False, compression="zstd")
    logger.info(f"写入完成: {filepath} ({len(df)} 行)")
    return filepath
