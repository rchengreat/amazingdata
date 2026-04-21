#!/bin/bash
# run_docker.sh — Docker 运行封装脚本，供 DSM 任务计划调用
#
# 用法：
#   ./run_docker.sh <script_name> [args...]
#
# 示例：
#   ./run_docker.sh fetch_info.py
#   ./run_docker.sh fetch_kline.py --type stock
#   ./run_docker.sh monthly_cleanup.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

IMAGE="amazingdata-fetcher:latest"
SCRIPT="$1"
shift || true  # remaining args passed to python script

LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${SCRIPT%.py}_$(date +%Y%m%d_%H%M%S).log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting: $SCRIPT $*" | tee -a "$LOG_FILE"

/usr/local/bin/docker run --rm \
  -v "$PROJECT_DIR/data:/volume1/amazingdata/data" \
  -v "$PROJECT_DIR/sdk_cache:/volume1/amazingdata/sdk_cache" \
  -v "$PROJECT_DIR/logs:/app/logs" \
  -v "$PROJECT_DIR/scripts:/app/scripts" \
  --env-file "$PROJECT_DIR/.env" \
  -e NUMBA_CACHE_DIR=/tmp/numba_cache \
  "$IMAGE" \
  python3 "scripts/$SCRIPT" "$@" 2>&1 | tee -a "$LOG_FILE"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Done: $SCRIPT" | tee -a "$LOG_FILE"
