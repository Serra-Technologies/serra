from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class PivotTransformer(Transformer):
    """
    A transformer to pivot a DataFrame based on specified row and column levels, and perform aggregation.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'row_level_column': The column used for row levels during pivoting.
                   - 'column_level_column': The column used for column levels during pivoting.
                   - 'value_column': The column to be summarized (values) during pivoting.
                   - 'aggregate_type': The type of aggregation to perform after pivoting.
                                      Should be one of 'avg' (average) or 'sum' (sum).
    """

    def __init__(self, config):
        self.config = config
        self.row_level_column = config.get("row_level_column")
        self.column_level_column = config.get("column_level_column")
        self.value_column = config.get("value_column")
        self.aggregate_type = config.get("aggregate_type")

    def transform(self, df):
        """
        Pivot the DataFrame based on the specified row and column levels, and perform aggregation.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame resulting from the pivot operation with the specified aggregation.
        :raises: SerraRunException if the specified aggregation type is invalid.
        """
        
        df = df.withColumn(self.value_column, F.col(self.value_column).cast("double"))
        df = df.groupBy(self.row_level_column).pivot(self.column_level_column)

        # Perform aggregation
        if self.aggregate_type == "avg":
            df = df.avg(self.value_column)
        elif self.aggregate_type == "sum":
            df = df.sum(self.value_column)
        else:
            raise SerraRunException("Invalid Pivot Aggregation type")

        return df


    