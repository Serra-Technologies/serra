import boto3
import pandas as pd
from serra.utils import get_or_create_spark_session
from serra.profile import get_serra_profile

class AmazonS3Reader():
    def __init__(self, config):
        self.config = config
        self.serra_profile = get_serra_profile()
    
    @property
    def bucket_name(self):
        return self.config.get("bucket_name")
    
    @property
    def file_path(self):
        return self.config.get("file_path")
    
    @property
    def file_type(self):
        return self.config.get("file_type")
    
    @property
    def dependencies(self):
        return []
    
    def read(self):
        session = boto3.Session(
            aws_access_key_id=self.serra_profile.aws_access_key_id,
            aws_secret_access_key=self.serra_profile.aws_secret_access_key
        )

        # Create an S3 client using the session
        s3_client = session.client('s3')

        # Read the CSV file from the S3 bucket using pandas
        # try:
        response = s3_client.get_object(Bucket=self.bucket_name, 
                                        Key=self.file_path)
        csv_data = response['Body']
        
        # Read the CSV data into a pandas DataFrame
        df = pd.read_csv(csv_data)
                
        # except Exception as e:
        #     print(f"Error reading CSV file: {str(e)}")

        spark = get_or_create_spark_session()
        spark_df = spark.createDataFrame(df)

        return spark_df
