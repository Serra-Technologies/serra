from serra.transformers import Transformer
from pyspark.sql.window import Window
from pyspark.sql import functions as F

class AddFirstTransformer(Transformer):
    """
    Adds a column to the dataframe which indicates whether it is the first of the condition
    """

    def __init__(self, output_column, condition):
        self.output_col = output_column
        self.condition = condition

    def transform(self, df):    
        df_temp = df.withColumn("condition_met", F.expr(self.condition))
        windowSpec = Window.partitionBy("final_amplitude_id", "condition_met").orderBy("event_time")
        df_ret = df_temp.withColumn("row_number", F.row_number().over(windowSpec)) \
            .withColumn(self.output_col, 
                        F.when((F.col("row_number") == F.min("row_number").over(windowSpec)) & \
                            (F.col("condition_met")), True) \
                        .otherwise(False)) \
            .drop("row_number", "condition_met")
        return df_ret
