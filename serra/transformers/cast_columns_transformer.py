from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CastColumnTransformer(Transformer):
    """
    A transformer to cast columns in a DataFrame to specified data types.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following key:
                   - 'cast_columns': A dictionary where the keys are the target column names
                                     and the values are lists containing the source column name
                                     and the target data type to which the source column will be cast.
                                     Example: {'target_column': ['source_column', 'target_data_type']}
    """

    def __init__(self, config):
        self.config = config
        self.column_dict = self.config.get("cast_columns")

    def transform(self, df):
        """
        Cast columns in the DataFrame to specified data types.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified columns cast to the target data types.
        """
        for target, [source, target_data_type] in self.column_dict.items():
            df = df.withColumn(target, F.col(source).cast(target_data_type))

        return df