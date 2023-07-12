import os

PACKAGE_PATH = os.path.dirname(__file__)

CONFIG_WORKSPACE_NAME="jobs" # Folder where all jobs will be placed

AWS_ACCESS_KEY_ID = "AKIA3TOV3GZZHAH4MPAE"
AWS_SECRET_ACCESS_KEY = "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"

AWS_BUCKET = "serrademo"

# The stuff below should probably be moved to the yaml file
DATABRICKS_HOST="dbc-b854a7df-4e5e.cloud.databricks.com"
DATABRICKS_TOKEN="dapi55908f285e31ab9937bc6928b4991fef"
DATABRICKS_CLUSTER_ID="0630-194840-lj2a32jr"

# WHEEL CONFIGS
PATH_TO_WHEEL = f"{PACKAGE_PATH}/../dist/serra-0.1-py3-none-any.whl"
WHEEL_FILE_NAME_IN_BUCKET = "serra-0.1-py3-none-any.whl"
S3_WHEEL_PATH = f"s3://serrademo/{WHEEL_FILE_NAME_IN_BUCKET}"

TEMPLATE_FOLDER = f"{PACKAGE_PATH}/frontend/public/"