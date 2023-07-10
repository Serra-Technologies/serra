from serra.readers import Reader
from serra.utils import get_or_create_spark_session_s3

class S3Reader(Reader):
    def __init__(self, config):
        self.spark = get_or_create_spark_session_s3()
        self.config = config
        self.path = self.config.get('path')
        self.format = self.config.get('format')
        
    def read(self):
        df = self.spark.read.format(self.format).option("header","true").load(self.path)
        return df


