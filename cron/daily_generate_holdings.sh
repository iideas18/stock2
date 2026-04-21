#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.generate_holdings_daily_job \
    >> data/log/generate_holdings.log 2>&1
