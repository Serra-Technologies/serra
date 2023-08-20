import boto3
import pandas as pd

from serra.readers import Reader

class AmazonS3Reader(Reader):
    """
    A class to read data from Amazon S3 and return a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'bucket_name': The name of the S3 bucket containing the file.
                   - 'file_path': The path to the file within the S3 bucket.
                   - 'file_type': The type of file to be read (e.g., 'csv', 'parquet').
    """

    def __init__(self, config):
        self.config = config
    
    @property
    def bucket_name(self):
        return self.config.get("bucket_name")
    
    @property
    def file_path(self):
        return self.config.get("file_path")
    
    @property
    def file_type(self):
        file_type = self.config.get("file_type")
        assert file_type in ['csv', 'parquet', 'json', 'orc']
        return self.config.get("file_type")
    
    @property
    def options(self):
        options = self.config.get('options')
        if not options:
            return {}
        # is dict
        return options
    
    @property
    def dependencies(self):
        return []
    
    def read(self):
        """
        Read data from Amazon S3 and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the S3 bucket.
        :raises: Any exceptions that may occur during file reading or DataFrame creation.
        """
        spark = self.spark
        s3_url = f"s3a://{self.bucket_name}/{self.file_path}"
        df_read = spark.read

        # To specify options like header: true
        for option_key, option_value in self.options.items():
            df_read = df_read.option(option_key, option_value)
            
        df = df_read.format(self.file_type).load(s3_url)

        return df
