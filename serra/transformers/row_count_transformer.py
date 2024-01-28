from serra.transformers import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

class RowCountTransformer(Transformer):
    """
    A transformer to add a column with the total row count of the DataFrame.
    """

    def __init__(self, output_column):
        """
        :param output_column: The name of the new column to store the row count.
        """
        self.output_column = output_column

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Add a column with the total row count to the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with an additional column containing the row count.
        """
        row_count = df.count()
        return df.withColumn(self.output_column, lit(row_count))