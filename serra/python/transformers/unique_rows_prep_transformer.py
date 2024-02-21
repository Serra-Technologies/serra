from serra.python.base import PythonTransformer

class UniqueTransformer(PythonTransformer):
    """
    Select specified columns

    :param columns: The columns to look for duplicate values in
    """

    def __init__(self, columns):
        self.columns = columns

    def transform(self, df):
        """
        If columns = [restaurant,id], then we want to drop rows that have both the same restaurant and id value

        """
        return df.drop_duplicates(self.columns)
