#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import pathlib,re,sys
errors=[]
for path in pathlib.Path('.').rglob('*.go'):
    if any(part.startswith('.') for part in path.parts):
        continue
    source=path.read_text()
    if 'Code generated' in source:
        continue
    for match in re.finditer(r'[A-Za-z_]"\s*\+\s*"[A-Za-z_]', source):
        line_number=source.count('\n',0,match.start())+1
        errors.append(f'{path}:{line_number}: string literal splits an identifier')
    for line_number,line in enumerate(source.splitlines(),1):
        if len(line)>140:
            errors.append(f'{path}:{line_number}: {len(line)} characters (maximum 140)')
print('\n'.join(errors))
if errors:sys.exit(1)
PY
# Pinned formatter checks standard library / third party / this module groups.
imports="$(go run github.com/daixiang0/gci@v0.13.7 list --skip-generated \
  -s standard -s default -s 'prefix(github.com/mindreon/orbit-control)' .)"
if [[ -n "$imports" ]]; then
  printf '%s\n' "$imports"
  exit 1
fi
go run ./scripts/stylecheck -tags temporal_integration,workflow_test_limits
