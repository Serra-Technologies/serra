from pyspark.sql.functions import col, when
from pyspark.sql.utils import AnalysisException
from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer

class MultiJoinTransformer(Transformer):
    """
    A transformer to join multiple DataFrames together based on specified join conditions.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'join_type': The type of join to perform. Currently only 'inner' join is supported.
                   - 'join_on': A dictionary where the keys are the table names (or DataFrame aliases)
                                and the values are the column names to join on for each table.
                   Example: {'table1': 'column1', 'table2': 'column2'}
    """


    def __init__(self, config):
        self.config = config
        self.join_type = config.get("join_type")
        self.join_on = config.get("join_on")

    # @property
    # def dependencies(self):
    #     """
    #     Get the list of table names that this transformer depends on.

    #     :return: A list of table names (keys from the 'join_on' dictionary).
    #     """
    #     return [key for key in self.config.get("join_on").keys()]

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
