from serra.python.base import PythonTransformer

class SortTransformer(PythonTransformer):
    """
    Create new columns of cumulative sums for given columns. Can group by and sort. 

    :param columns: Columns to calculate sums.
    :param group_by: Column(s) to group by.
    :param sort_by: Column(s) to sort by.
    """

    def __init__(self, sort_by, ascending):
        self.sort_by = sort_by
        self.ascending = ascending.lower() == "true"

    def transform(self, df):
        return df.sort_values(by = self.sort_by, ascending = self.ascending)
