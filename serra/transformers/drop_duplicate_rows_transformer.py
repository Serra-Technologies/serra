from serra.transformers import Transformer
from pyspark.sql import DataFrame

class DropDuplicateRowsTransformer(Transformer):
    """
    A transformer to drop duplicate rows in a DataFrame.
    """

    def __init__(self):
        pass

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Drop duplicate rows in the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with duplicate rows removed.
        """
        return df.dropDuplicates()