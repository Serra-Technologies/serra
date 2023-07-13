from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CastColumnTransformer(Transformer):

    def __init__(self, config):
        self.config = config
        self.column_dict = self.config.get("cast_columns")

    def transform(self, df):
        for target, [source, target_data_type] in self.column_dict.items():
            df = df.withColumn(target, F.col(source).cast(target_data_type))

        return df