#!/bin/bash
# migrate_from_tgw.sh — 一次性把 TGW 历史数据复制到 amazingdata/data 目录
#
# 用法（在 NAS 上执行）：
#   sudo bash /volume1/amazingdata/migrate_from_tgw.sh
#
# 说明：
#   - 只复制 amazingdata 项目需要的文件（不含 TGW 独有文件）
#   - 如果目标文件已存在，跳过（不覆盖已有的 amazingdata 产出）
#   - extra_*_history.parquet 复制后可作为增量基础，daily 文件不复制

set -euo pipefail

SRC="/volume1/tgw"
DST="/volume1/amazingdata/data"

mkdir -p "$DST"

FILES=(
    "info_stock_basic.parquet"
    "info_stock_factor.parquet"
    "info_industry_basic_history.parquet"
    "info_industry_detail_history.parquet"
    "info_index_detail_history.parquet"
    "info_index_weight_history.parquet"
    "equity_structure_history.parquet"
    "equity_dividend_history.parquet"
    "finance_balance_sheet_history.parquet"
    "finance_cash_flow_history.parquet"
    "finance_income_history.parquet"
    "margin_summary_history.parquet"
    "margin_detail_history.parquet"
    "extra_etf_history.parquet"
    "extra_index_history.parquet"
    "extra_stock_history.parquet"
)

for f in "${FILES[@]}"; do
    src_path="$SRC/$f"
    dst_path="$DST/$f"
    if [ ! -f "$src_path" ]; then
        echo "[SKIP] 源文件不存在: $src_path"
        continue
    fi
    if [ -f "$dst_path" ]; then
        src_size=$(stat -c%s "$src_path" 2>/dev/null || stat -f%z "$src_path")
        dst_size=$(stat -c%s "$dst_path" 2>/dev/null || stat -f%z "$dst_path")
        echo "[SKIP] 目标已存在: $f (src=${src_size} dst=${dst_size})"
        continue
    fi
    echo "[COPY] $f ..."
    cp "$src_path" "$dst_path"
    echo "[DONE] $f"
done

echo ""
echo "=== 完成 ==="
ls -lh "$DST"
