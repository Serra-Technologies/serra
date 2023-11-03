from serra.readers import Reader

class S3Reader(Reader):
    """
    A class to read data from Amazon S3 and return a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'bucket_name': The name of the S3 bucket containing the file.
                   - 'file_path': The path to the file within the S3 bucket.
                   - 'file_type': The type of file to be read (e.g., 'csv', 'parquet').
    """

    def __init__(self, bucket_name, file_path, file_type, options):
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_type = file_type
        self.options = options
    
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
        if self.options is not None:
            for option_key, option_value in self.options.items():
                df_read = df_read.option(option_key, option_value)
            
        df = df_read.format(self.file_type).load(s3_url)

        return df
