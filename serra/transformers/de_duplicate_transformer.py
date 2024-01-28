from serra.transformers import Transformer
from pyspark.sql import Window
from pyspark.sql import functions as F

class DeDuplicateTransformer(Transformer):
    def __init__(self, columns_to_compare, time_frame):
        self.col_to_compare = columns_to_compare
        self.columns_to_compare = columns_to_compare
        self.time_frame = int(time_frame)

    def transform(self, df):
        df = df.repartition(*self.columns_to_compare)
        df = df.withColumn("event_time", F.to_timestamp("event_time"))
        window_spec = Window.partitionBy(self.columns_to_compare).orderBy("event_time")
        df = df.withColumn("rn", F.row_number().over(window_spec))
        df = df.withColumn("row_diff",
                    F.unix_timestamp(F.col("event_time")) - F.lag(F.unix_timestamp(F.col("event_time")), 1).over(window_spec))
        time_diff = 60 * self.time_frame
        df = df.filter((F.col("row_diff") > time_diff) | F.col("row_diff").isNull())
        df = df.drop("rn", "row_diff")

        return df
