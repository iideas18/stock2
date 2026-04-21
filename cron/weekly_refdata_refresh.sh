#!/usr/bin/env bash
# Run weekly (Sunday 02:00) to refresh industry / listing / st reference data.
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m instock.job.industry_refresh_job   >> data/log/refdata.log 2>&1
python -m instock.job.listing_refresh_job    >> data/log/refdata.log 2>&1
python -m instock.job.st_refresh_job         >> data/log/refdata.log 2>&1
