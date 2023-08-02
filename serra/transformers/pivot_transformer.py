from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class PivotTransformer(Transformer):
    """
    A transformer to pivot a DataFrame based on specified row and column levels, and perform aggregation.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'row_level': The column used for row levels during pivoting.
                   - 'column_level': The column used for column levels during pivoting.
                   - 'sum_col': The column to be summarized (values) during pivoting.
                   - 'aggregate_type': The type of aggregation to perform after pivoting.
                                      Should be one of 'avg' (average) or 'sum' (sum).
    """

    def __init__(self, config):
        self.config = config
        self.row_level = config.get("row_level")
        self.column_level = config.get("column_level")
        self.sum_col = config.get("sum_col")
        self.aggregate_type = config.get("aggregate_type")

    def transform(self, df):
        """
        Pivot the DataFrame based on the specified row and column levels, and perform aggregation.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame resulting from the pivot operation with the specified aggregation.
        :raises: SerraRunException if the specified aggregation type is invalid.
        """
        
        df = df.withColumn(self.sum_col, F.col(self.sum_col).cast("double"))
        df = df.groupBy(self.row_level).pivot(self.column_level)

        # Perform aggregation
        if self.aggregate_type == "avg":
            df = df.avg(self.sum_col)
        elif self.aggregate_type == "sum":
            df = df.sum(self.sum_col)
        else:
            raise SerraRunException("Invalid Pivot Aggregation type")

        return df


    