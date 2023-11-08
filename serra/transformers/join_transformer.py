from pyspark.sql import functions as F

from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer

class JoinTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'join_type': The type of join to perform. Currently only 'inner' join is supported.
                   - 'join_on': A dictionary where the keys are the table names (or DataFrame aliases)
                                and the values are the column names to join on for each table.
                   Example: {'table1': 'column1', 'table2': 'column2'}
    """


    def __init__(self, join_type, join_on):
        self.join_type = join_type
        self.join_on = join_on
    
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
        # assert self.join_type in "inner"

        join_keys = []

        
        for table in self.join_on:
            join_keys.append(self.join_on.get(table))

        assert len(join_keys) == 2

        matching_col = join_keys

        df1 = df1.join(df2, df1[matching_col[0]] == df2[matching_col[1]], self.join_type).drop(df2[matching_col[1]])
        if df1.isEmpty():
            raise SerraRunException(f"""Joiner - Join Key Error: {matching_col[0]}, {matching_col[1]} columns do not match.""")
        return df1
    
    