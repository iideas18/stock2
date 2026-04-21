#!/usr/bin/env bash
# */15 * * * * - run the monitoring checks
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.monitoring_check_job \
    >> data/log/monitoring_check.log 2>&1
