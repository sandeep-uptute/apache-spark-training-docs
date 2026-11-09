# Ubuntu 22.04 Preflight — Spark 4.1 / OpenJDK 17 / Python 3.12.3

**Goal:** Verify the *existing* environment and ensure the notebook's Python kernel, PySpark, Java and worker processes agree. Avoid reinstalling a working environment on training day.

## 1. Confirm the real versions

In an Ubuntu terminal:

```bash
cat /etc/os-release | grep -E '^(NAME|VERSION)='
java -version
python3.12 --version
python3.12 -c 'import sys; print(sys.executable)'
```

Expected operating system is Ubuntu 22.04.x. Expected Java major version is 17, Python exactly 3.12.3 (or check if patch differs), PySpark major/minor **4.1**. Ubuntu 22.04 originally ships Python 3.10, so a working Python 3.12.3 may have been installed separately. **Do not replace the system Python** to match it.

## 2. Use one isolated Python environment (if one is not already available)

If the current Jupyter kernel already has matching PySpark, you can keep it. Otherwise, create a virtual environment with the **existing** Python 3.12 interpreter:

```bash
cd /path/to/apache_spark_4_1_ubuntu_training
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install 'pyspark==4.1.0' 'jupyterlab>=4,<5' 'ipykernel>=6,<8'
python -m ipykernel install --user --name spark41-ubuntu --display-name 'Python 3.12 (PySpark 4.1)'
```

`4.1.0` is a concrete example. If your already working environment is **4.1.1/4.1.2/etc.**, keep that patch version rather than forcing a downgrade. If `python3.12 -m venv` is unavailable, the Python 3.12 installation may need the matching venv support; obtain it through the method that supplied your Python 3.12 interpreter, rather than modifying Ubuntu's default Python. A network connection is needed for first-time `pip` installation.

**Important:** You can use a pip-installed PySpark runtime or an existing Spark distribution, but avoid mixing two different Spark installations in the same notebook kernel. In particular, check `SPARK_HOME`, the running notebook's Python interpreter, and the package version if results seem inconsistent.

## 3. Check Java selection and Spark identity

```bash
readlink -f "$(command -v java)"
java -version
python -c 'import pyspark; print(pyspark.__version__, pyspark.__file__)'
```

If Java 17 is on `PATH`, set `JAVA_HOME` only if your current configuration requires it. On standard Ubuntu x86_64 installs the location is often `/usr/lib/jvm/java-17-openjdk-amd64`, but **derive it** rather than hard-coding:

```bash
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
echo "$JAVA_HOME"
```

If both Java 17 and newer JDKs are installed, make sure the resolved `java` is actually Java 17 before testing. Jupyter must be launched from the same activated environment, or the registered **Python 3.12 (PySpark 4.1)** kernel must be selected explicitly.

## 4. Execute the real integration smoke test

From the workshop root using your intended Python environment:

```bash
python scripts/validate_package.py
python scripts/smoke_test.py
```

A successful smoke test reports `PASS` after exercising **CSV, JSON, DataFrame transformations, joins, groupings, and Parquet read/write**. It uses a temporary directory, so no workshop outputs are overwritten.

## 5. Launch Jupyter

```bash
# From the project root, with the correct venv activated:
./scripts/launch_jupyter.sh
```

Select the registered Python 3.12 / Spark 4.1 kernel, then execute `notebooks/02_getting_started.ipynb` before training. The first cell prints the Spark and Python versions.

## 6. Troubleshooting: common real-world problems

| Symptom | Likely cause | Action |
|---|---|---|
| Notebook reports the wrong PySpark version | Kernel uses another interpreter | Inspect `sys.executable` in notebook; select the correct kernel; avoid conflicting `SPARK_HOME` |
| `JAVA_GATEWAY_EXITED` or JVM start failure | Java selection or hostname resolution | Check `java -version`, `JAVA_HOME`; inspect Java error text; on local machines with unresolvable hostname, try `SPARK_LOCAL_IP=127.0.0.1` before Jupyter launch |
| Python workers fail to connect | Different driver and worker Python | Set `PYSPARK_PYTHON` to the notebook's `sys.executable` **before** SparkSession creation; restart kernel if Spark was already started |
| `FileNotFoundError` for data | Launched from wrong directory | Start Jupyter in the workshop root; the notebook searches cwd and parents for `data/transactions.csv` |
| DataFrame `cast()` fails on bad input | Spark ANSI casting behavior | Prefer `try_cast` for intentionally malformed data in these exercises |
| Out of memory / machine becomes slow | Too many local threads / concurrent notebooks | Use `local[2]`; keep `spark.sql.shuffle.partitions=4`; close idle notebooks; do not `collect()` large datasets |
| Can't find a single Parquet output file | Spark writes dataset directories | Read/write the **folder**; the files inside are `part-*` plus metadata |
| Workshop package extraction errors | Missing folder / modified data | Run `validate_package.py`; re-extract without renaming nested folders |

**No separate Hadoop service is required** for this workshop. On Ubuntu there is no need to install Windows-only `winutils.exe` or `hadoop.dll`. Local PySpark includes its client-side Spark/Hadoop integration dependencies. For HDFS or YARN you would need additional infrastructure, but those systems are not part of this syllabus.

## 7. Last-minute fallback

If an individual trainee's local setup fails, do not hold up the session: pair them with a working participant for the demonstrations and have them finish setup after the workshop. Keep one presenter machine and one local backup with the full zip extracted and the smoke test passing. All source datasets are included, so running notebooks does not require an active internet connection after installation.
