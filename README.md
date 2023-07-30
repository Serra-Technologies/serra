![Project Header](serra.png)

Translate SQL to Object-Oriented Spark

## What is Serra?
Developers can retool long-winded SQL scripts into simple, object-oriented Spark code with one command `serra translate`. 

Serra is an end-to-end, ETL framework that simplifies complex SQL scripts to a few lines of PySpark code with transformer and in-house connector objects.

Serra provides fully-customizable error logging and local testing for every transformer and connector.

With a command line tool, developers can easily translate their existing SQL scripts to get the full benefit of object-oriented Spark, create pipelines, auto document them, run local tests, and run jobs in Databricks.


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


# AWS S3 Local Setup

### Step 1: Install AWS CLI
```bash
pip install awscli
```

### Step 2: Configure AWS Credentials
```bash
aws configure
```
* Fill out the credentials as so:
```
AWS Access Key ID: [YOUR ACCESS KEY]
AWS Secret Access Key: [YOUR SECRET ACCESS KEY]
Default region name: [YOUR DEFAULT S3 REGION]
Default output format: [DEFAULT FORMAT]
```

### Step 3: Update workspace_examples/profiles.yml
* Update your credentials like you did in Step 2:
```
AWS Access Key ID: [YOUR ACCESS KEY]
AWS Secret Access Key: [YOUR SECRET ACCESS KEY]
```


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