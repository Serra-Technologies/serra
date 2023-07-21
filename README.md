# Serra
Object-Oriented Spark Framework for Data Transformations

# Setup for running

```bash
python3.10 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install -e .
```

# May also need to follow these steps (Download and install spark)
https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading

# Development Guide

## If you make changes to the package ( not just a new config)

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
DB Workspace: https://dbc-b854a7df-4e5e.cloud.databricks.com
DB Token: dapiaca3916d64c45c3b454fd6bb0e5a20c3
cluster_id: 0630-194840-lj2a32jr
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


# Run the flask server
```bash
source python3 -m venv env
python3 app.py
```