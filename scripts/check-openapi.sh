#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
npm exec --yes --package=@redocly/cli@1.34.5 -- redocly lint docs/openapi.yaml --extends=minimal
