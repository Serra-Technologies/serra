import os
import json

import boto3

from serra.utils import get_local_serra_profile

def get_s3_client():
    user_profile = get_local_serra_profile()
    s3 = boto3.resource('s3',
            aws_access_key_id=user_profile.aws_access_key_id,
            aws_secret_access_key=user_profile.aws_secret_access_key)
    return s3

def upload_file_to_config_bucket(file_path):
    bucket = get_local_serra_profile().aws_config_bucket
    upload_file_to_bucket(file_path, bucket)

def retrieve_file_from_config_bucket(filename):
    bucket = get_local_serra_profile().aws_config_bucket
    data = retrieve_file_as_bytes_from_bucket(filename, bucket)
    return data

# General AWS helper fxns
def upload_file_to_bucket(file_path, bucket):
    name = file_path.split("/")[-1]
    with open(file_path, 'rb') as data:
        get_s3_client().Bucket(bucket).put_object(Key=name, Body=data)

def retrieve_file_as_bytes_from_bucket(filename, bucket):
    obj = get_s3_client().Object(bucket_name=bucket, key=filename)
    response = obj.get()
    data = response['Body'].read()
    return data

def read_json_s3(file, bucket):
    s3 = get_s3_client()
    content_object = s3.Object(bucket, f'{file}.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)

def write_json_s3(obj, file, bucket):
    s3 = get_s3_client()
    json_dump_s3 = lambda obj, f: s3.Object(bucket, key=f).put(Body=json.dumps(obj))
    json_dump_s3(obj, f'{file}.json')

def copy_folder_to_s3(local_folder_path, bucket_name, s3_folder_key):
    """Copy a local folder and its contents to an S3 bucket."""
    s3_client = boto3.client("s3")

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            s3_file_key = os.path.join(s3_folder_key, relative_path)
            s3_client.upload_file(local_file_path, bucket_name, s3_file_key)

