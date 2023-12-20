from serra.transformers import Transformer
from pyspark.sql.window import Window
from pyspark.sql import functions as F

class AddFirstTransformer(Transformer):
    """
    Adds a column to the dataframe which indicates whether it is the first of the condition
    """

    def __init__(self, output_col, condition):
        self.output_col = output_col
        self.condition = condition

    def transform(self, df):
        windowSpec = Window.partitionBy("final_amplitude_id").orderBy("event_time")

        df_ret = df.withColumn("row_number", F.row_number().over(windowSpec)) \
            .withColumn("condition_met", F.expr(self.condition)) \
            .withColumn(self.output_col, 
                        F.when((F.col("row_number") == F.min("row_number").over(windowSpec.partitionBy("condition_met"))) & \
                            (F.col("condition_met")), True) \
                        .otherwise(False)) \
            .drop("row_number", "condition_met")
        return df_ret
