#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ -z "${VIRTUAL_ENV:-}" ]; then
  echo "Warning: no virtualenv active. Recommended: python -m venv .venv && source .venv/bin/activate"
fi
python3 scripts/scan.py --config config.yaml
