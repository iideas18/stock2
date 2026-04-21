#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.backtest_run_job --strategy demo \
    --start "$(date -d '90 days ago' +%Y-%m-%d)" \
    --end   "$(date +%Y-%m-%d)" \
    >> data/log/backtest_run.log 2>&1
