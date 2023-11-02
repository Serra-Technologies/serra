from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class JoinWithConditionTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param join_type: The type of join to perform. Currently only 'inner' join is supported.
    :param condition: The condition for the join.
    :param join_on: A dictionary where the keys are the table names (or DataFrame aliases)
                    and the values are the column names to join on for each table.
                    Example: {'table1': 'column1', 'table2': 'column2'}
    """

    def __init__(self, join_type, condition, join_on):
        self.join_type = join_type
        self.condition = condition
        self.join_on = join_on

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        
        df1 = df1.alias('a').join(df2.alias('b'), F.expr(self.condition), self.join_type)

        return df1
    
    