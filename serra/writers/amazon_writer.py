import boto3
from serra.profile import get_serra_profile


class AmazonS3Writer():
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
        return [self.config.get('input_block')]
    
    def write(self, df):
        # Convert DataFrame to CSV data
        pandas_df = df.toPandas()
        csv_data = pandas_df.to_csv(index=False)
        
        session = boto3.Session(
            aws_access_key_id=self.serra_profile.aws_access_key_id,
            aws_secret_access_key=self.serra_profile.aws_secret_access_key
        )

        # Create an S3 client using the session
        s3_client = session.client('s3')
        
        # Write the CSV data to the S3 bucket
        try:
            response = s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.file_path,
                Body=csv_data.encode('utf-8')
            )
            print(f"CSV file written to {self.bucket_name}/{self.file_path}")
            
        except Exception as e:
            print(f"Error writing CSV file: {str(e)}")
