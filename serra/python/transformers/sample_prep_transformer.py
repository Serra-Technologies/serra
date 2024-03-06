from serra.python.base import PythonTransformer

class SampleTransformer(PythonTransformer):
    """
    Gets sample of size N from dataset.

    :param size: Sample size
    :param type: Type of sample (first/last N rows, 1 of every 100 rows, 1 in 100 chance to include each row)
    """

    def __init__(self, size, type):
        self.size = int(size)
        self.type = type

    def transform(self, df):
        type_map = {
            "First N rows": df.head,
            "Last N rows": df.tail,
            "Random sample": df.sample
            # "1 in 100 chance to include each row": randomsample
        }

        sample_function = type_map.get(self.type)

        df = sample_function(self.size)
        return df
