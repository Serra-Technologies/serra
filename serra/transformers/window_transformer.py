from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from serra.transformers.transformer import Transformer

class WindowTransformer(Transformer):
    """
    A transformer to set a window for all other steps after it.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'partition_by': A list of column names for partitioning the window.
                   - 'order_by': A list of column names for ordering within the window.
                   - 'window_name': The name of the window column to be added.
    """

    def __init__(self, config):
        self.config = config
        self.partition_by = config.get("partition_by", [])


    def transform(self, df: DataFrame) -> DataFrame:
        window_spec = Window().partitionBy(*self.partition_by).orderBy(*self.partition_by)
        df_with_window = df.withColumn(f"window_{self.partition_by[0]}", F.row_number().over(window_spec))
        return df_with_window
