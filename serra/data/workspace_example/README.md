![Project Header](./etc/serra.png)

Translate SQL to Object-Oriented Spark

## What is Serra?
Developers can retool long-winded SQL scripts into simple, object-oriented Spark code with one command `serra translate`. 

Serra is an end-to-end, ETL framework that simplifies complex SQL scripts to a few lines of PySpark code with transformer and in-house connector objects.

Serra provides fully-customizable error logging and local testing for every transformer and connector.

With a command line tool, developers can easily translate their existing SQL scripts to get the full benefit of object-oriented Spark, create pipelines, auto document them, run local tests, and run jobs in Databricks.

# Getting Started
Create a new directory and run an interactive bash shell with docker to get started with all the required dependencies:

```bash
mkdir serra-dev
cd serra-dev
docker run --mount type=bind,source="$(pwd)",target=/app -it serraio/serra /bin/bash
```

Run `serra create` to create a workspace folder. 

```bash
serra create
```

Navigate to the workspace folder and run your first job!

```bash
cd workspace
serra run Demo
```

Other jobs available can be found in the **workspace_example/jobs** folder.

# Connector Credentials
Update your credentials for AWS, Databricks, and Snowflake in `workspace/profiles.yml`

```
AWS_ACCESS_KEY_ID: [YOUR ACCESS KEY]
AWS_SECRET_ACCESS_KEY: [YOUR SECRET ACCESS KEY]

AWS_CONFIG_BUCKET: ENTER_HERE # Bucket to use to place job config files (not needed for quickstart)

DATABRICKS_HOST: ENTER_HERE
DATABRICKS_TOKEN: ENTER_HERE
DATABRICKS_CLUSTER_ID: ENTER_HERE

SNOWFLAKE:
  USER: ENTER_HERE
  PASSWORD: ENTER_HERE
  ACCOUNT: ENTER_HERE (Organization-Account)
```

Now your jobs can connect between AWS, Databricks, and Snowflake data sources!

# SQL to Serra LLM (Beta)
Translate monolithic SQL scripts to low-code, Serra spark configuration files with one line.

```bash
cd workspace
serra translate hard_demo.sql
```
Place your sql scripts in **workspace_example/sql** folder.

# Command Line Tool
Translate, test locally, and run Databricks jobs with single commands.

## Translate
```bash
serra translate {sql_file}.sql
```

## Test Locally
```bash
serra run {job_name}
```
Your job name is what you name your configuration file. Place your configuration files in **workspace_example/jobs** folder.


## Deploy to Databricks
```bash
serra deploy {job_name}
```
Run your job configuration files directly on Databricks. 

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

### Step 3: Update workspace_examples/profiles.yml

* Update with same credentials from Step 2:
```
DB Workspace: https://your-workspace.cloud.databricks.com
DB Token: your_token
cluster_id: your_cluster_id
```

### Step 4: Confirm connection
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
