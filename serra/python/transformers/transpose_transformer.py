import pandas as pd

from serra.python.base import PythonTransformer

class TransposeTransformer(PythonTransformer):
    """
    Gets sample of size N from dataset.

    :param size: Sample size
    :param type: Type of sample (first/last N rows, 1 of every 100 rows, 1 in 100 chance to include each row)
    """

    def __init__(self, columns):
        self.columns = columns

    def transform(self, df):
        df = pd.melt(df[self.columns],var_name='Name',value_name='Value')
        return df
