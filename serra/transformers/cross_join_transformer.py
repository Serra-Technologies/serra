from pyspark.sql import functions as F

from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer

class CrossJoinTransformer(Transformer):
    """
    A transformer to join two DataFrames together
    """

    def __init__(self):
        pass

    @property
    def dependencies(self):
        return self.input_block

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        
        return df1.alias('a').crossJoin(df2.alias('b'))
    
    