#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="${SPARK_LAB_DIR:-$HOME/spark-projects}"
VENV="$WORKSPACE/spark-env"
SPARK_VERSION=4.1.4

info() { printf '[INFO] %s\n' "$*"; }
fail() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }
trap 'printf "[ERROR] Installer stopped at line %s (exit %s). Rerun the same command after fixing the reported issue.\n" "$LINENO" "$?" >&2' ERR

[[ $(uname -s) == Linux ]] || fail 'Linux is required.'
[[ -r /etc/os-release ]] || fail 'Cannot identify the Linux distribution.'
# shellcheck disable=SC1091
source /etc/os-release
[[ ${ID:-} == ubuntu ]] || fail "This package is for Ubuntu (found ${PRETTY_NAME:-unknown})."
[[ $(uname -m) == x86_64 ]] || fail 'This installer targets x86_64 Ubuntu.'
command -v python3.12 >/dev/null || fail 'Python 3.12 is required; it will not be replaced by this script.'
python3.12 -c 'import sys; assert sys.version_info[:2] == (3, 12), sys.version'
command -v apt-get >/dev/null || fail 'apt-get was not found.'
command -v flock >/dev/null || fail 'flock is required (normally included in Ubuntu).'

mkdir -p "$WORKSPACE"
# Prevent two installers from changing the same virtual environment concurrently.
exec 9>"$WORKSPACE/.pyspark-install.lock"
flock -n 9 || fail "An installation is already running for $WORKSPACE"

java_ok() {
  dpkg-query -W -f='${Status}' openjdk-17-jdk 2>/dev/null | grep -qx 'install ok installed' &&
  [[ -x /usr/lib/jvm/java-17-openjdk-amd64/bin/java && -x /usr/lib/jvm/java-17-openjdk-amd64/bin/javac ]]
}
venv_pkg_ok() {
  dpkg-query -W -f='${Status}' python3.12-venv 2>/dev/null | grep -qx 'install ok installed'
}
if ! java_ok || ! venv_pkg_ok; then
  info 'Installing only missing Ubuntu prerequisites. sudo may request a password.'
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-17-jdk python3.12-venv
fi
java_ok || fail 'OpenJDK 17 installation is incomplete.'
venv_pkg_ok || fail 'python3.12-venv installation is incomplete.'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
info "Using $($JAVA_HOME/bin/java -version 2>&1 | head -1)"

# Ownership marker distinguishes an interrupted installer-created venv from an
# arbitrary directory. Never --clear or rm -rf an existing venv.
MARKER="$VENV/.managed-by-ubuntu-pyspark-setup"
if [[ -e "$VENV" && ! -d "$VENV" ]]; then
  fail "$VENV exists but is not a directory. Choose SPARK_LAB_DIR."
fi
if [[ ! -d "$VENV" ]]; then
  info "Preparing new virtual environment: $VENV"
  mkdir "$VENV"
  printf 'managed-venv-v1\n' > "$MARKER"
fi
if [[ ! -f "$VENV/pyvenv.cfg" || ! -x "$VENV/bin/python" ]]; then
  [[ -f "$MARKER" ]] || fail "Existing incomplete environment $VENV is not managed by this installer. Won't overwrite it."
  info 'Creating or repairing interrupted virtual-environment creation.'
  python3.12 -m venv "$VENV"
fi
"$VENV/bin/python" -c 'import sys; assert sys.version_info[:2] == (3,12), sys.version; assert sys.prefix != sys.base_prefix, "not in a venv"' || fail 'Existing venv is not a functional Python 3.12 venv; choose a different SPARK_LAB_DIR.'
"$VENV/bin/python" -m pip --version >/dev/null || {
  [[ -f "$MARKER" ]] || fail 'Existing venv has no pip and is not installer-managed.'
  info 'Repairing pip in managed environment.'
  "$VENV/bin/python" -m ensurepip --upgrade
}

spark_ok() {
  "$VENV/bin/python" -c 'from importlib.metadata import version; assert version("pyspark") == "4.1.4"' >/dev/null 2>&1
}
if spark_ok; then
  info "PySpark $SPARK_VERSION is already installed; skipping download."
else
  info "Installing/resuming pinned PySpark $SPARK_VERSION (first download is ~456 MB)."
  "$VENV/bin/python" -m pip install --disable-pip-version-check "pyspark==$SPARK_VERSION"
  spark_ok || fail 'PySpark installation/version verification failed.'
fi

# Install notebook tooling into the same isolated Python environment.
# Use bounded major versions; skip installation entirely when already satisfied.
notebook_ok() {
  "$VENV/bin/python" - <<'PYCODE' >/dev/null 2>&1
from importlib.metadata import version
from packaging.version import Version
assert Version('4.0') <= Version(version('jupyterlab')) < Version('5.0')
assert Version('6.0') <= Version(version('ipykernel')) < Version('8.0')
assert Version('3.8') <= Version(version('matplotlib')) < Version('4.0')
PYCODE
}
if notebook_ok; then
  info 'JupyterLab, IPython kernel and Matplotlib already installed; skipping download.'
else
  info 'Installing/resuming JupyterLab, IPython kernel and Matplotlib in the same Spark venv.'
  "$VENV/bin/python" -m pip install --disable-pip-version-check 'jupyterlab>=4,<5' 'ipykernel>=6,<8' 'matplotlib>=3.8,<4'
  notebook_ok || fail 'JupyterLab/ipykernel/Matplotlib installation check failed.'
fi

# Register an explicit, named kernel within the virtual environment.
# Running again updates the kernel registration without creating duplicates.
KERNEL_DIR="$VENV/share/jupyter/kernels/pyspark-412"
if [[ -f "$KERNEL_DIR/kernel.json" ]] && KERNEL_DIR="$KERNEL_DIR" VENV="$VENV" "$VENV/bin/python" - <<'PYCODE' >/dev/null 2>&1
import json, os
from pathlib import Path
kernel=json.loads((Path(os.environ['KERNEL_DIR'])/'kernel.json').read_text())
assert kernel['argv'][0] == os.environ['VENV'] + '/bin/python'
PYCODE
then
  info 'PySpark notebook kernel already registered.'
else
  info 'Registering PySpark notebook kernel.'
  "$VENV/bin/python" -m ipykernel install --sys-prefix --name pyspark-412 --display-name 'PySpark 4.1.4 (Python 3.12)'
fi

info 'Running end-to-end filesystem read/write tests (fresh temporary files).'

SPARK_LAB_DIR="$WORKSPACE" bash "$SCRIPT_DIR/02-validate.sh"
# Status only written after successful validation; actual state is always checked on rerun.
printf 'Spark=%s\nJava=17\nPython=3.12\nMatplotlib=installed\nValidation=passed\n' "$SPARK_VERSION" > "$WORKSPACE/.pyspark-setup-status"
info 'SUCCESS: Ready for PySpark exercises.'
printf 'Start shell: bash %q\n' "$SCRIPT_DIR/03-start-pyspark.sh"
printf 'Start JupyterLab: bash %q\n' "$SCRIPT_DIR/04-start-jupyter.sh"
