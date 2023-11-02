from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class SelectTransformer(Transformer):
    """
    A transformer to perform a SELECT operation on a DataFrame.

    :param columns: A list of column names to select from the DataFrame.
    :param distinct_column: Optional. The column for which distinct values should be retained.
    :param filter_expression: Optional. A filter expression to apply to the DataFrame.
    """

    def __init__(self, columns=None, distinct_column=None, filter_expression=None):
        self.columns = columns or []
        self.distinct_column = distinct_column
        self.filter_expression = filter_expression

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame containing only the selected columns.
        :raises: SerraRunException if no columns are specified in the configuration
                 or if none of the specified columns exist in the DataFrame.
        """
        if not self.columns:
            columns = []

        selected_columns = [F.col(col) for col in self.columns if col in df.columns]

        if not selected_columns:
            raise SerraRunException("None of the specified columns exist in the DataFrame.")

        df = df.select(*selected_columns)
        
        if self.distinct_column is not None:
            df = df.dropDuplicates(self.distinct_column)

        if self.filter_expression is not None:
            df = df.filter(F.expr(self.filter_expression))
        return df



