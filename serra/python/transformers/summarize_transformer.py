from serra.python.transformers.transformer import Transformer

class SummarizeTransformer(Transformer):
    """
    Gets sample of size N from dataset.

    :param size: Sample size
    :param type: Type of sample (first/last N rows, 1 of every 100 rows, 1 in 100 chance to include each row)
    """

    def __init__(self, group_by, input_columns_and_actions):
        self.group_by = group_by
        self.input_columns_and_actions = input_columns_and_actions

    def transform(self, df):
        df = df.groupby(self.group_by, as_index=False)
        return df.agg(self.input_columns_and_actions)
