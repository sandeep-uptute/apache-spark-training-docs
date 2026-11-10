#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="${SPARK_LAB_DIR:-$HOME/spark-projects}"
VENV="$WORKSPACE/spark-env"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
[[ -x "$JAVA_HOME/bin/java" && -x "$JAVA_HOME/bin/javac" ]] || { echo '[ERROR] Missing OpenJDK 17 JDK.' >&2; exit 1; }
[[ -x "$VENV/bin/python" ]] || { echo "[ERROR] Missing Python venv: $VENV" >&2; exit 1; }
export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
"$VENV/bin/python" - <<'PY'
import sys
from importlib.metadata import version
assert sys.version_info[:2] == (3, 12), sys.version
assert version('pyspark') == '4.1.4', f"Wrong PySpark version: {version('pyspark')}"
print('Python:', sys.version.split()[0], '| PySpark:', version('pyspark'))
PY
"$VENV/bin/python" - <<'PYCODE'
from importlib.metadata import version
from packaging.version import Version
assert Version('4.0') <= Version(version('jupyterlab')) < Version('5.0')
assert Version('6.0') <= Version(version('ipykernel')) < Version('8.0')
assert Version('3.8') <= Version(version('matplotlib')) < Version('4.0')
import jupyterlab, ipykernel, matplotlib
from jupyter_client.kernelspec import KernelSpecManager
ks = KernelSpecManager(kernel_dirs=[__import__('sys').prefix + '/share/jupyter/kernels']).get_kernel_spec('pyspark-412')
assert ks.argv[0] == __import__('sys').executable, (ks.argv, __import__('sys').executable)
print('[PASS] JupyterLab, Matplotlib and registered PySpark kernel')
PYCODE
"$JAVA_HOME/bin/java" -version
"$VENV/bin/python" "$SCRIPT_DIR/validate_spark.py"
echo '[PASS] Spark execution and filesystem I/O verified.'
