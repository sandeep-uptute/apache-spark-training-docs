#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if ! command -v java >/dev/null 2>&1; then echo 'ERROR: java not on PATH' >&2; exit 1; fi
if ! python -c 'import pyspark,jupyterlab,ipykernel' 2>/dev/null; then
  echo 'ERROR: Activate the prepared Python environment with PySpark, JupyterLab and ipykernel.' >&2; exit 1
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="$(readlink -f "$(command -v java)")"
  export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
fi
export PYSPARK_PYTHON="$(python -c 'import sys; print(sys.executable)')"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
cd "$ROOT"
exec python -m jupyter lab --notebook-dir="$ROOT"
