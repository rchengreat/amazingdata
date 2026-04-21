#!/bin/bash
# git_pull.sh — 从 GitHub 拉取最新代码
# DSM 任务计划每小时运行一次

set -euo pipefail

REPO_DIR="/volume1/amazingdata"
LOG="$REPO_DIR/logs/git_pull.log"

mkdir -p "$REPO_DIR/logs"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] git pull..." | tee -a "$LOG"

cd "$REPO_DIR"
git fetch origin main 2>&1 | tee -a "$LOG"

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 已是最新，无需更新" | tee -a "$LOG"
else
    git reset --hard origin/main 2>&1 | tee -a "$LOG"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 更新完成: $LOCAL -> $REMOTE" | tee -a "$LOG"
fi
