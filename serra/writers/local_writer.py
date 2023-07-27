from serra.writers.writer import Writer
from serra.utils import get_or_create_spark_session

class LocalWriter(Writer):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.file_path = config.get("file_path")
    
    def write(self, df):
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # Write the Pandas DataFrame to a local file
        pandas_df.to_csv(self.file_path, index=False)