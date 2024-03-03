import boto3
from serra.python.base import PythonReader
import pandas as pd
from io import StringIO

class S3Reader(PythonReader):
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, bucket_name: str, path: str, region_name: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name
        self.path = path
        self.region_name = region_name
    
    def read(self):
        s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name)
        obj = s3.get_object(Bucket=self.bucket_name, Key=self.path)
        stream = obj['Body']
        data = stream.read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        return df