from pyspark.sql import functions as F
from serra.exceptions import SerraRunException
from serra.transformers.transformer import Transformer
from serra.readers.s3_reader import S3Reader
import logging

logger = logging.getLogger(__name__)

class JoinTransformer(Transformer):
    """
    Join tables together
    :param join_type: Type of join
    :param df1, df2: dataframes
    :param matching_col: column to join on
    """

    def __init__(self, config):
        self.config = config
        self.join_type = config.get("join_type")
        self.join_on = config.get("join_on")

    @property
    def dependencies(self):
        return [key for key in self.config.get("join_on").keys()]

    def transform(self, df1, df2):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """
        assert self.join_type in "inner"

        join_keys = []
        for table in self.join_on:
            join_keys.append(self.join_on.get(table))

        assert len(join_keys) == 2

        matching_col = join_keys

        df1 = df1.join(df2, df1[matching_col[0]] == df2[matching_col[1]], self.join_type).drop(df2[matching_col[1]])
        if df1.isEmpty():
            raise SerraRunException(f"""Joiner - Join Key Error: {matching_col[0]}, {matching_col[1]} columns do not match.""")
        return df1
    
    