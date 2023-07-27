# For now assumes csv file
from serra.readers import Reader
from serra.utils import get_or_create_spark_session
from pyspark.sql import SparkSession

class LocalReader(Reader):
    def __init__(self, config):
        self.spark: SparkSession = get_or_create_spark_session()
        self.config = config
        self.file_path = config.get("file_path")
        
    def read(self):
        df = self.spark.read.format("csv").option("header",True).load(self.file_path)
        return df


