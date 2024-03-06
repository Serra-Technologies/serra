from serra.python.base import PythonTransformer

class JoinTransformer(PythonTransformer):
    """
    Join two datasets together (defaults to inner join). Can be used to union datasets. 

    :param right_df: Second dataframe in join
    :param left_df_key: First dataframe's join key
    :param right_df_key: Second dataframe's join key
    :param join_type: Type of join (inner, right, left, outer/union). Defaults to inner. 
    """

    def __init__(self, left_df, right_df, left_df_key, right_df_key, join_type = "inner"):
        self.left_df = left_df
        self.right_df = right_df
        self.left_df_key = left_df_key
        self.right_df_key = right_df_key
        self.join_type = join_type

    def transform(self):
        return self.left_df.merge(self.right_df, left_on=self.left_df_key, right_on=self.right_df_key, how=self.join_type)
