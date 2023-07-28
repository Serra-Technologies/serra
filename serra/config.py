import os

PACKAGE_PATH = os.path.dirname(__file__)

# Used to place config files, wheel files
AWS_BUCKET = "serrademo"

# WHEEL CONFIGS
PATH_TO_WHEEL = f"{PACKAGE_PATH}/../dist/serra-0.1-py3-none-any.whl"
WHEEL_FILE_NAME_IN_BUCKET = "serra-0.1-py3-none-any.whl"
S3_WHEEL_PATH = f"s3://{AWS_BUCKET}/{WHEEL_FILE_NAME_IN_BUCKET}"
