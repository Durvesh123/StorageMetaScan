#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
echo "Using Python: $(which python)"
python3 -m scripts.scan --config config.yaml


