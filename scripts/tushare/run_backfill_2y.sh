#!/usr/bin/env bash
# 一次性：元数据 + 清空并回填近 2 年 daily_kline
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p logs
if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

echo ">>> 1/2 instrument 元数据"
python3 instrument_meta_update.py 2>&1 | tee logs/instrument_meta.log

echo ">>> 2/2 daily_kline 回填（20240716 至今，断点续传）"
python3 daily_kline_update.py --resume --start 20240716 --workers 4 2>&1 | tee logs/backfill_2y.log

echo "完成"
