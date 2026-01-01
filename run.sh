
#!/usr/bin/env bash
set -euo pipefail
python -m snow_ai run --input "$1" --out outputs ${2:+--ci-lookup "$2"}
