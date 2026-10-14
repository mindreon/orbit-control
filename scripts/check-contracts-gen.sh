#!/usr/bin/env bash
# Fail when internal/contract/contract_gen.go is not what scripts/gen-contracts.sh generates from contract/v2/schema.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

bash "$root/scripts/gen-contracts.sh" "$tmp/contract_gen.go"
if ! diff -u "$root/internal/contract/contract_gen.go" "$tmp/contract_gen.go"; then
  echo "internal/contract/contract_gen.go is stale: run scripts/gen-contracts.sh and commit the result" >&2
  exit 1
fi
echo "contract_gen.go is up to date"
