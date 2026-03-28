#!/bin/bash

set -euo pipefail

INPUT_PATH="${1:-/input/data}"

echo "Running full indexing flow"
echo "Input path: $INPUT_PATH"

bash create_index.sh "$INPUT_PATH"
bash store_index.sh
