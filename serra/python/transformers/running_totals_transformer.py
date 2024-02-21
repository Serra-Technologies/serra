import pandas as pd

from serra.python.base import PythonTransformer

class RunningTotalsTransformer(PythonTransformer):
    """
    Create new columns of cumulative sums for given columns. Can group by and sort. 

    :param columns: Columns to calculate sums.
    :param group_by: Column(s) to group by.
    :param sort_by: Column(s) to sort by.
    """

    def __init__(self, columns, group_by = None, sort_by = None):
        self.columns = columns
        self.group_by = group_by
        self.sort_by = sort_by

    def transform(self, df):
        """
        First, create cumulative column names with list comphrension.
        Second, use these new column names to assign the cumulative sums to (group by first if needed).
        Third, sort.
        """
        cum_columns = ['cum_' + col for col in self.columns]

        if self.group_by != None:
            df[cum_columns] = df.groupby(self.group_by)[self.columns].cumsum()
        else:
            df[cum_columns] = df[self.columns].cumsum()

        if self.sort_by != None:
            df = df.sort_values(by = self.sort_by)

        return df