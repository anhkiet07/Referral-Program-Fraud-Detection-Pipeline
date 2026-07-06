#!/usr/bin/env bash
# Runs data profiling followed by the main fraud-detection pipeline.
# Both steps read from $INPUT_DIR and write to subfolders of $REPORT_OUTPUT_DIR's
# parent (see Dockerfile ENV defaults), which should be a mounted host volume
# so results are visible outside the container.
set -euo pipefail

echo "=================================================================="
echo "STEP 1/2: Data Profiling"
echo "=================================================================="
python3 src/profiling.py --input-dir "${INPUT_DIR}" --output-dir "${PROFILING_OUTPUT_DIR}"

echo
echo "=================================================================="
echo "STEP 2/2: Referral Fraud Detection Pipeline"
echo "=================================================================="
python3 src/referral_pipeline.py --input-dir "${INPUT_DIR}" --output-dir "${REPORT_OUTPUT_DIR}"

echo
echo "Done. Reports are available under the mounted output volume."
