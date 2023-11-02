from pyspark.sql.functions import col

from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer

class MultiJoinTransformer(Transformer):
    """
    A transformer to join multiple DataFrames together based on specified join conditions.

    :param join_type: The type of join to perform. Currently only 'inner' join is supported.
    :param join_on: A dictionary where the keys are the table names (or DataFrame aliases)
                    and the values are the column names to join on for each table.
                    Example: {'table1': 'column1', 'table2': 'column2'}
    """

    def __init__(self, join_type, join_on):
        self.join_type = join_type
        self.join_on = join_on

    def transform(self, *dfs):
        """
        Join multiple DataFrames together based on the specified join conditions.

        :param dfs: A variable number of DataFrames to be joined.
        :return: A new DataFrame resulting from the join operations.
        :raises: SerraRunException if the join condition columns do not match between the DataFrames.
        """  

        join_keys = []
        for table in self.join_on:
            join_keys.append(self.join_on.get(table))
            

        matching_col = join_keys

        joined_df = dfs[0]
        for i in range(1,len(dfs)):
            if matching_col[i-1] != matching_col[i]:
                joined_df = joined_df.join(dfs[i], joined_df[matching_col[i-1]] == dfs[i][matching_col[i]], self.join_type[i-1])
            else:
                joined_df = joined_df.join(dfs[i], matching_col[i], self.join_type[i-1])

            non_null_counts = [joined_df.where(col(c).isNotNull()).count() for c in dfs[i].columns]
            if joined_df.isEmpty() or all(count == 0 for count in non_null_counts):
                raise SerraRunException(f"""Joiner - Join Key Error: {matching_col[0]}, {matching_col[1]} columns do not match.""")

        return joined_df
