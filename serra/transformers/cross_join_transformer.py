from pyspark.sql import functions as F

from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer

class CrossJoinTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'join_type': The type of join to perform. Currently only 'inner' join is supported.
                   - 'join_on': A dictionary where the keys are the table names (or DataFrame aliases)
                                and the values are the column names to join on for each table.
                   Example: {'table1': 'column1', 'table2': 'column2'}
    """


    def __init__(self, config):
        self.config = config

    # @property
    # def dependencies(self):
    #     """
    #     Get the list of table names that this transformer depends on.

    #     :return: A list of table names (keys from the 'join_on' dictionary).
    #     """
    #     return [key for key in self.config.get("join_on").keys()]
    @property
    def dependencies(self):
        return self.config.get('input_block')

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        
        return df1.alias('a').crossJoin(df2.alias('b'))
    
    