from serra.python.transformers.transformer import Transformer
import pandas as pd

class CountRowsTransformer(Transformer):
    """
    Get the row count for the dataset.

    """

    def __init__(self):
        pass
        
    def transform(self, df):
        row_count = len(df.index)
        df = pd.DataFrame({'Count': [row_count]})
        return df
