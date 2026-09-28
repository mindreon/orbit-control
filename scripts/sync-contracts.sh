#!/usr/bin/env bash
# Copy contract v3 from an orbit-runtime checkout and regenerate the Go types.
# Usage: scripts/sync-contracts.sh <path-to-orbit-runtime>
set -euo pipefail

runtime="${1:?usage: scripts/sync-contracts.sh <path-to-orbit-runtime>}"
src="$runtime/schema/v3"
dst="$(cd "$(dirname "$0")/.." && pwd)/internal/contract/v3"

for f in contracts.json examples.json; do
  [[ -f "$src/$f" ]] || { echo "missing $src/$f" >&2; exit 1; }
done

cp "$src/contracts.json" "$dst/contracts.json"
cp "$src/examples.json" "$dst/testdata/examples.json"
(cd "$dst" && go generate ./...)
echo "synced contract v3 from $src"
