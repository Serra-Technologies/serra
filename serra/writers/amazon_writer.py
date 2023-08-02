import boto3

from serra.profile import get_serra_profile

class AmazonS3Writer():
    """
    A writer to write data from a Spark DataFrame to Amazon S3 as a CSV file.

    :param config: A dictionary containing the configuration for the writer.
                   It should have the following keys:
                   - 'bucket_name': The name of the S3 bucket to write to.
                   - 'file_path': The path to the CSV file in the S3 bucket.
                   - 'file_type': The file type, which should be 'csv' for this writer.
                   - 'input_block': The name of the input block used as a dependency for this writer.
    """
    
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
        """
        Write data from a Spark DataFrame to Amazon S3 as a CSV file.

        :param df: The Spark DataFrame to be written.
        """
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
