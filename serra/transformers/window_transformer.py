from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from serra.transformers.transformer import Transformer

class WindowTransformer(Transformer):
    """
    A transformer to set a window for all other steps after it.

    :param partition_by: A list of column names for partitioning the window.
    :param order_by: A list of column names for ordering within the window.
    :param window_name: The name of the window column to be added.
    """

    def __init__(self, partition_by=None, order_by=None, window_name=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []
        self.window_name = window_name

    @classmethod
    def from_config(cls, config):
        partition_by = config.get("partition_by", [])
        order_by = config.get("order_by", [])
        window_name = config.get("window_name")

        obj = cls(partition_by, order_by, window_name)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df: DataFrame) -> DataFrame:
        window_spec = Window().partitionBy(*self.partition_by).orderBy(*self.partition_by)
        df_with_window = df.withColumn(f"window_{self.partition_by[0]}", F.row_number().over(window_spec))
        return df_with_window
