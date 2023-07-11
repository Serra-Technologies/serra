# Serra
Object-Oriented Spark Framework for Data Transformations

# Setup

```bash
brew install python3.8
python3.8 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install -r requirements_dev.txt
pip install -e .
```

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



# Run on databricks
serra-databricks StripeExample