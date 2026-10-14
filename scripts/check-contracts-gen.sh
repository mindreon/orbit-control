#!/usr/bin/env bash
# Fail when internal/contract/contract_gen.go is not what scripts/gen-contracts.sh generates from contract/v2/schema.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

bash "$root/scripts/gen-contracts.sh" "$tmp/contract_gen.go"
for name in contract_gen.go contract_secret_gen.go contract_registry_gen.go contract_types_gen_test.go; do
  diff -u "$root/internal/contract/$name" "$tmp/$name"
done
echo "contract types, secrets, registry and constructor table are up to date"
