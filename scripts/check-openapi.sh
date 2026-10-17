#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -gt 0 ]]; then
  if [[ $# -ne 2 || "$1" != "--edge-config" ]]; then
    printf 'usage: %s [--edge-config <Caddyfile>]\n' "$0" >&2
    exit 2
  fi
  # Go regexp matches Caddy's RE2 syntax; require every gateway-owned route.
  go run ./scripts/edgecheck --edge-config "$2" --openapi docs/openapi.yaml
fi
# Redocly 1.x rejects JSON Schema's boolean `then`/`else` schemas. Lint an
# equivalent temporary representation while retaining the canonical contract refs.
check_dir="$(mktemp -d "$PWD/.openapi-lint.XXXXXX")"
trap 'rm -rf "$check_dir"' EXIT
mkdir -p "$check_dir/docs" "$check_dir/contract/v2"
cp docs/openapi.yaml "$check_dir/docs/"
cp -R contract/v2/schema "$check_dir/contract/v2/"
python3 - "$check_dir" <<'PY'
import json, pathlib, sys

def normalize(value):
    if isinstance(value, dict):
        return {key: ({} if item else {"not": {}}) if key in ("then", "else") and isinstance(item, bool)
                else normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value

for path in pathlib.Path(sys.argv[1], "contract/v2/schema").glob("*.json"):
    path.write_text(json.dumps(normalize(json.loads(path.read_text())), indent=2))
PY
npm exec --yes --package=@redocly/cli@1.34.5 -- redocly lint "$check_dir/docs/openapi.yaml" --extends=minimal
