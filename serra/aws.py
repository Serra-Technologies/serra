import boto3
import json
from serra.profile import get_serra_profile

user_profile = get_serra_profile()
s3 = boto3.resource('s3',
        aws_access_key_id=user_profile.aws_access_key_id,
        aws_secret_access_key=user_profile.aws_secret_access_key)

def upload_file_to_config_bucket(file_path):
    bucket = user_profile.aws_config_bucket
    upload_file_to_bucket(file_path, bucket)

def retrieve_file_from_config_bucket(filename):
    bucket = user_profile.aws_config_bucket
    data = retrieve_file_as_bytes_from_bucket(filename, bucket)
    return data

# General AWS helper fxns
def upload_file_to_bucket(file_path, bucket):
    name = file_path.split("/")[-1]
    with open(file_path, 'rb') as data:
        s3.Bucket(bucket).put_object(Key=name, Body=data)

def retrieve_file_as_bytes_from_bucket(filename, bucket):
    obj = s3.Object(bucket_name=bucket, key=filename)
    response = obj.get()
    data = response['Body'].read()
    return data

def read_json_s3(file, bucket):
    content_object = s3.Object(bucket, f'{file}.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)

def write_json_s3(obj, file, bucket):
    json_dump_s3 = lambda obj, f: s3.Object(bucket, key=f).put(Body=json.dumps(obj))
    json_dump_s3(obj, f'{file}.json')