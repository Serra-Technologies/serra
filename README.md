# Serra
![Project Header](serra.png)

Translate SQL to Object-Oriented Spark

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Serra.

```bash
pip install serra==0.3
```

# Setup

Setup your virtual environment below.

```bash
python3.10 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install -e .
```

or

```bash
source run.sh
```

# Getting Started
Navigate to the workspace_example folder and try out a couple of jobs!

```bash
cd workspace_example
serra run LocalExample
```
Other jobs available can be found in the **workspace_example/jobs** folder.

# SQL to Serra LLM (Beta)
Translate monolithic SQL scripts to low-code, Serra spark configuration files with one line.

```bash
cd workspace_example
serra translate hard_demo.sql
```
Place your sql scripts in **workspace_example/sql** folder.


# Databricks Development Guide

## If you make changes to the package (not just a new config)

### Step 1: Create wheel
```bash
source env/bin/activate
python setup.py bdist_wheel
```
* NOTE: Wheel should be found in dist directory after running this.

### Step 2: Upload wheel to s3 for access from AWS
```bash
serra update_package
```
* NOTE: This may take around a minute to also restart the databricks cluster

## If you add a new job ( new confg file)
```bash
serra create_job {job_name}
```

# Databricks Local Setup

### Step 1: Install DB-connect
```bash
pip3 install --upgrade "databricks-connect==12.2.*"
```

### Step 2: Configure w/ DB cluster
```bash
databricks-connect configure
```
* Fill out the credentials as so:
```
DB Workspace: https://your-workspace.cloud.databricks.com
DB Token: your_token
cluster_id: your_cluster_id
```

### Step 3: Confirm connection
* To test if your connection is setup
```bash
databricks-connect test
```

* All local spark sessions can now read from DB ie
```python
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SELECT * FROM demo.sales_by_store")
```