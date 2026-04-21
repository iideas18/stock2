#!/usr/bin/env bash
# Daily factor computation (run after market close, e.g. 18:00 local)
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.factor_compute_daily_job \
    >> data/log/factor_compute.log 2>&1
