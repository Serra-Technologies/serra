from pyspark.sql import functions as F

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
        self.matching_col = config.get("matching_col")
        self.path = config.get("path")


    def transform(self, df1):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        matching_col = self.matching_col

        df2_config = {'path':self.path, 'format':'csv'}
        df2 = S3Reader(df2_config).read()
        df1 = df1.join(df2, df1[matching_col[0]] == df2[matching_col[1]], self.join_type).drop(df2[matching_col[1]])
        if df1.isEmpty():
            logger.error(f"Joiner - Invalid matching ID's: {matching_col[0]}, {matching_col[1]}")
            raise Exception(f"Joiner - Invalid matching ID's: {matching_col[0]}, {matching_col[1]}")
        return df1
    
    