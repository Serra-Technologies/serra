from serra.transformers import Transformer
from pyspark.sql import DataFrame

class DropNullTransformer(Transformer):
    """
    A transformer to drop rows with null values in specified columns.

    :param columns: A list of column names to check for null values.
    """

    def __init__(self, columns):
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Drop rows with null values in specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with rows containing null values dropped.
        """
        for column in self.columns:
            df = df.filter(df[column].isNotNull())
        return df