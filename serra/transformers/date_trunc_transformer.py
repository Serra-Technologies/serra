from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import TimestampType
from datetime import datetime



from pyspark.sql import functions as F
from serra.transformers.transformer import Transformer

class DateTruncTransformer(Transformer):
    """
    A transformer to truncate a timestamp column to a specified unit.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'timestamp_col': The name of the timestamp column to be truncated.
                   - 'trunc_unit': The unit for truncating the timestamp (e.g., 'day', 'month', 'year').
    """

    def __init__(self, config):
        self.config = config
        self.timestamp_col = config.get("timestamp_col")
        self.trunc_unit = config.get("trunc_unit")
        self.output_col = config.get('output_col')

    def transform(self, df):
        """
        Truncate the specified timestamp column to the specified unit.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the truncated timestamp column.
        """
        dt = F.to_timestamp(self.timestamp_col, "yyyy-MM-dd HH:mm:ss")
        
        if self.trunc_unit == "day":
            truncated_time = F.date_trunc("day", dt)
        elif self.trunc_unit == "month":
            truncated_time = F.date_trunc("month", dt)
        elif self.trunc_unit == "year":
            truncated_time = F.date_trunc("year", dt)
        else:
            truncated_time = None
        
        return df.withColumn(self.output_col, truncated_time)
