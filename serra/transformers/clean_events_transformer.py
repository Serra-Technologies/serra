from serra.transformers import Transformer
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

class CleanEventsTransformer(Transformer):
    """
    Removes events if they satisfy a current_row_condition and there is a row in the comparison range
    that satisifies the other_row_condition. The comparison range is specified by comparison_range_start
    and comparison_range_end specifying the range in minutes.

    Creates temporary columns:
    - event_time_unix
    - remove_flag

    Expects columns:
    - final_amplitude_id
    - event_time
    - event_type
    """

    def __init__(self,
                range_start,
                range_end,
                current_row_condition,
                other_row_condition):
        self.range_start = int(range_start) # minutes before current row
        self.range_end = int(range_end) # minutes after current row
        self.current_row_condition = current_row_condition
        self.other_row_condition = other_row_condition

    def transform(self, df: DataFrame):
        # Convert event_time to Unix timestamp (seconds)
        df = df.withColumn("event_time_unix", F.unix_timestamp("event_time"))

        # Define the window specification with Unix timestamp
        windowSpec = Window.partitionBy("final_amplitude_id").orderBy("event_time_unix").rangeBetween(self.range_start * 60, self.range_end * 60)

        # Flag sessions for removal
        df = df.withColumn("remove_flag",
                        F.when(
                            (F.expr(self.current_row_condition)) & 
                            (F.max(
                                F.when(F.expr(self.other_row_condition),F.lit(1))
                                .otherwise(F.lit(0))
                                ).over(windowSpec) == 1),
                            F.lit(1)
                        ).otherwise(F.lit(0)))

        # Filter out flagged sessions and drop temporary columns
        filtered_df = df.filter(F.col("remove_flag") == 0).drop("remove_flag", "event_time_unix")
        return filtered_df