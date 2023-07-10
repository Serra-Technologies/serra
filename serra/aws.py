import boto3
from serra.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET

# s3 setup
s3 = boto3.resource('s3',
         aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def upload_file_to_bucket(file_path):
    name = file_path.split("/")[-1]
    with open(file_path, 'rb') as data:
        s3.Bucket(AWS_BUCKET).put_object(Key=name, Body=data)

def retrieve_file_as_bytes_from_bucket(filename):
    obj = s3.Object(bucket_name=AWS_BUCKET, key=filename)
    response = obj.get()
    data = response['Body'].read()
    return data