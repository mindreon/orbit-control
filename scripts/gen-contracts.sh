#!/usr/bin/env bash
# Generate the Go types of contract v2 from contract/v2/schema.
# Usage: scripts/gen-contracts.sh [output-file]   (default: internal/contract/contract_gen.go)
# Needs network access to the Go module proxy the first time; set GOPROXY if the default proxy lacks the tool.
set -euo pipefail

# Pinned: a different version formats and names types differently.
go_jsonschema="github.com/atombender/go-jsonschema@v0.24.1"

root="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-$root/internal/contract/contract_gen.go}"

cd "$root"
go run "$go_jsonschema" -p contract -o "$out" contract/v2/schema/*.json
