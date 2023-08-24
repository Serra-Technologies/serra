from pyspark.sql.functions import col, explode

from serra.transformers.transformer import Transformer

class ExplodeJsonTransformer(Transformer):
    """
    A transformer to filter the DataFrame based on a specified condition.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'condition': A list of values to filter the DataFrame by.
                   - 'column': The name of the column to apply the filter on.
    """

    def __init__(self, config):
        self.config = config
        self.nested_col = self.config.get('nested_col')

    def transform(self, df):
        """
        Filter the DataFrame based on the specified condition.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with rows filtered based on the specified condition.
        """
        # Read and process the text file line by line
        col_list = list(df.select(self.nested_col).collect()[0][self.nested_col][0].keys())
        select_cols = [col(f"{self.nested_col}.{col_name}") for col_name in col_list]

        df = df.select("*", explode(col(self.nested_col))).drop(self.nested_col)
        df = df.withColumnRenamed("col",self.nested_col)
        df = df.select("*", *select_cols).drop(self.nested_col)

        return df