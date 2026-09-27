#!/usr/bin/env bash
# Scan a directory for secret-like values: DB URLs, the one-time CI DB
# passwords, planted E2E secrets and common key shapes. Set SCAN_CANARY to add
# one more literal (used by the CI positive control).
# Prints only file names, never the matched value. Exit 1 on any hit.
set -euo pipefail
dir=${1:?usage: ci-secret-scan.sh <dir>}
pattern='postgres(ql)?://|ci-owner|ci-app|ci-ops|ci-superuser|e2e-planted|sk-live|BEGIN [A-Z ]*PRIVATE KEY|AKIA[0-9A-Z]{16}'
if [ -n "${SCAN_CANARY:-}" ]; then
  pattern="${pattern}|${SCAN_CANARY}"
fi
hits=$(grep -rlE -- "$pattern" "$dir" || true)
if [ -n "$hits" ]; then
  echo "secret-like value found in:"
  echo "$hits"
  exit 1
fi
echo "clean: $(find "$dir" -type f | wc -l) files under $dir"
