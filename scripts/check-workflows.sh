#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
go run go.temporal.io/sdk/contrib/tools/workflowcheck@v0.6.0 ./internal/workflows/...
go test ./internal/workflows/... -run TestReplay -count=1
