#!/usr/bin/env bash
set -Eeuo pipefail
WORKSPACE="${SPARK_LAB_DIR:-$HOME/spark-projects}"
VENV="$WORKSPACE/spark-env"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$VENV/bin:$PATH"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
[[ -x "$JAVA_HOME/bin/java" ]] || { echo '[ERROR] Missing OpenJDK 17.' >&2; exit 1; }
[[ -x "$VENV/bin/python" && -x "$VENV/bin/jupyter" ]] || { echo '[ERROR] Missing JupyterLab venv. Run 01-install.sh.' >&2; exit 1; }
export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
mkdir -p "$WORKSPACE/notebooks"
cd "$WORKSPACE"
# Local-only listening: do not expose an unauthenticated notebook server to the network.
exec "$VENV/bin/python" -m jupyterlab --no-browser --ip=127.0.0.1 --port=8888 --ServerApp.root_dir="$WORKSPACE" "$@"
