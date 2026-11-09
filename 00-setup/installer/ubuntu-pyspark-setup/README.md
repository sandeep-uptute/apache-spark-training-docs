# Ubuntu PySpark 4.1.4 installer

Target: Ubuntu x86_64 with **Python 3.12.x already installed**. Installs OpenJDK 17 and `python3.12-venv` via apt only when needed. Installs PySpark 4.1.4 plus JupyterLab 4 and a dedicated notebook kernel in an isolated `~/spark-projects/spark-env` virtual environment. Standalone Hadoop, `winutils.exe`, and `hadoop.dll` are not required for local exercises.

## One-command setup

Unzip, change to this directory, and run:

```bash
bash 01-install.sh
```

The installer requires internet for apt/pip, permission to run `sudo` when prerequisites are missing, and sufficient free disk space for the ~456 MB PySpark download, installation, and temporary files. It does **not** modify shell startup files or your system Python. Installation can be rerun after failure, cancellation, or reboot using the **same command**.

## Run after setup

```bash
bash 02-validate.sh     # independently rerun read/write tests
bash 03-start-pyspark.sh # interactive shell
bash 04-start-jupyter.sh # JupyterLab, local-only at http://127.0.0.1:8888
```

## Behavior on repeat execution / after interruption

- Uses a workspace-specific `flock` lock to reject concurrent installers.
- Checks apt prerequisites and skips apt operations when already installed.
- Reuses an existing Python 3.12 virtual environment; repairs interrupted creation **only** when its ownership marker proves this installer created it. It never deletes an existing virtual environment.
- Checks the exact installed PySpark version and skips `pip` when matched. If installation was interrupted, rerunning `pip install` completes/retries it, using pip's cache when possible.
- Runs a fresh end-to-end test on every invocation using a disposable directory that cleans up automatically. Previously generated work is not touched.
- Writes `.pyspark-setup-status` only after validation succeeds. The status file is informational, not used to bypass dependency checks.
- A loss of internet connection, insufficient disk space, broken apt packages, or inconsistent external virtual environment may require manual correction. This is *retry-safe stage-based resume*, **not** byte-level continuation of interrupted network downloads.

## Optional workspace

```bash
SPARK_LAB_DIR="$HOME/my-spark-lab" bash 01-install.sh
SPARK_LAB_DIR="$HOME/my-spark-lab" bash 03-start-pyspark.sh
```

## Troubleshooting

- `python3.12: command not found`: install Python 3.12 first; no automatic Python version changes.
- `Unable to locate package python3.12-venv`: verify Ubuntu package repositories; Ubuntu releases may differ in package availability.
- `Permission denied`: check that you own your workspace; do not run the installer itself with sudo.
- JVM hostname resolution: for local use, scripts default to `SPARK_LOCAL_IP=127.0.0.1`. This is **not** a network-accessible cluster setup.
- Disk/network failure: fix the problem and rerun `bash 01-install.sh`.
- Pre-existing incompatible venv: set `SPARK_LAB_DIR` to a fresh location, preserving the original.

## Validation coverage

Python tuples -> Spark DataFrame, Spark transformation, CSV/TSV/newline-delimited JSON reads, multiline JSON reads, CSV/TSV/JSON/Parquet writes and read-back, and a Spark job triggered for each format. Output directories contain Spark `part-*` files; this is expected.

## Testing note

Bash syntax and Python compilation can be verified offline. Full JVM/Spark integration testing **must** be run on the target Ubuntu machine; it is not implied by successful syntax checks.

## Notebook workflow

- Run `bash 04-start-jupyter.sh`, then open the tokenized localhost URL printed in the terminal (or paste it into your browser). Keep that terminal running. Ctrl+C stops JupyterLab.
- Pick kernel **PySpark 4.1.4 (Python 3.12)**. Open `example_pyspark.ipynb` from this extracted setup folder or copy it into `~/spark-projects/notebooks/`.
- Notebooks use the same `~/spark-projects/spark-env` environment as the PySpark shell. No manual `source .../activate` is needed when using either launcher.
- Jupyter notebooks do not automatically supply a `spark` variable: create a `SparkSession` at the start of each notebook (shown in example).
- Launch notebook and shell as separate processes. For limited-memory Ubuntu machines, stop one before doing large Spark jobs in the other.
- The Jupyter server is deliberately bound to `127.0.0.1` and uses token authentication; remote SSH users should use SSH port forwarding rather than `--ip=0.0.0.0`.
- `SPARK_LAB_DIR` may override the workspace for all four scripts.
- Adding Jupyter after an earlier Spark-only installation is safe: rerun `bash 01-install.sh`; it skips the existing Java and Spark steps.
