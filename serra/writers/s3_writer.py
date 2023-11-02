from serra.writers import Writer

class S3Writer(Writer):
    """
    A writer to write data from a Spark DataFrame to Amazon S3 as a CSV file.

    :param config: A dictionary containing the configuration for the writer.
                   It should have the following keys:
                   - 'bucket_name': The name of the S3 bucket to write to.
                   - 'file_path': The path to the CSV file in the S3 bucket.
                   - 'file_type': The file type, which should be 'csv' for this writer.
                   - 'input_block': The name of the input block used as a dependency for this writer.
    """
    
    def __init__(self, bucket_name, file_path, file_type, options, mode):
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_type = file_type
        self.options = options
        self.mode = mode
    
    def write(self, df):
        """
        Write data from a Spark DataFrame to Amazon S3 as a CSV file.

        :param df: The Spark DataFrame to be written.
        """
        df_write = df.write

        # To specify options like header: true
        if self.options is not None:
            for option_key, option_value in self.options.items():
                df_write = df_write.option(option_key, option_value)
        
        s3_url = f"s3a://{self.bucket_name}/{self.file_path}"
        df_write.mode(self.mode).format(self.file_type).save(s3_url)
