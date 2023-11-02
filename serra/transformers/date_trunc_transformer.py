from pyspark.sql import functions as F
from serra.transformers.transformer import Transformer

class DateTruncTransformer(Transformer):
    """
    A transformer to truncate a timestamp column to a specified unit.

    :param timestamp_column: The name of the timestamp column to be truncated.
    :param trunc_unit: The unit for truncating the timestamp (e.g., 'day', 'month', 'year').
    :param output_column: The name of the new column to create with the truncated timestamps.
    """

    def __init__(self, timestamp_column, trunc_unit, output_column):
        self.timestamp_column = timestamp_column
        self.trunc_unit = trunc_unit
        self.output_column = output_column

    def transform(self, df):
        """
        Truncate the specified timestamp column to the specified unit.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the truncated timestamp column.
        """
        dt = F.to_timestamp(self.timestamp_column, "yyyy-MM-dd HH:mm:ss")
        
        if self.trunc_unit == "day":
            truncated_time = F.date_trunc("day", dt)
        elif self.trunc_unit == "month":
            truncated_time = F.date_trunc("month", dt)
        elif self.trunc_unit == "year":
            truncated_time = F.date_trunc("year", dt)
        else:
            truncated_time = None
        
        return df.withColumn(self.output_column, truncated_time)
