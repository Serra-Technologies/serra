from serra.transformers.transformer import Transformer

class OrderByTransformer(Transformer):
    """
    A transformer to sort the DataFrame based on specified columns in ascending or descending order.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'cols': A list of column names to sort the DataFrame by.
                   - 'ascending': Optional. If True (default), sort in ascending order.
                                  If False, sort in descending order.
    """

    def __init__(self, config):
        self.config = config
        self.cols = config.get("cols")
        self.ascending = config.get("ascending")

        if self.ascending is None:
            self.ascending = True

    def transform(self, df):
        """
        Transform the DataFrame by sorting it based on specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the rows sorted based on the specified columns.
        """

        return df.orderBy(*self.cols, ascending = self.ascending)
