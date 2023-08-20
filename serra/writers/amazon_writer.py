from serra.writers import Writer

class AmazonS3Writer(Writer):
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
    def options(self):
        options = self.config.get('options')
        if not options:
            return {}
        return options
    
    @property
    def mode(self):
        mode = self.config.get('mode')
        assert mode in ['error', 'overwrite', 'ignore', 'errorifexists']
        return mode
    
    @property
    def dependencies(self):
        return [self.config.get('input_block')]
    
    def write(self, df):
        """
        Write data from a Spark DataFrame to Amazon S3 as a CSV file.

        :param df: The Spark DataFrame to be written.
        """
        df_write = df.write

        # To specify options like header: true
        for option_key, option_value in self.options.items():
            df_write = df_write.option(option_key, option_value)
        
        s3_url = f"s3a://{self.bucket_name}/{self.file_path}"
        df_write.mode(self.mode).format(self.file_type).save(s3_url)
