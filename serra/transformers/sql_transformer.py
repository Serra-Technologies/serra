from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class SQLTransformer(Transformer):
    """
    A transformer to perform a SELECT operation on a DataFrame.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following key:
                   - 'columns': A list of column names to select from the DataFrame.
    """

    def __init__(self, config):
        self.config = config
        self.sql_expression = config.get('sql_expression')

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame containing only the selected columns.
        :raises: SerraRunException if no columns are specified in the configuration
                 or if none of the specified columns exist in the DataFrame.
        """
        df = df.filter(F.expr(self.sql_expression))
        return df



