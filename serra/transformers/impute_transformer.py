from pyspark.ml.feature import Imputer
from pyspark.sql import functions as F
from serra.transformers.transformer import Transformer

class ImputeTransformer(Transformer):
    """
    A transformer to clean/impute missing values in specified columns.

    :param columns_to_impute: A list of column names to impute missing values.
    :param imputation_strategy: The imputation strategy. Supported values: 'mean', 'median', 'mode'.
    """

    def __init__(self, columns_to_impute, imputation_strategy):
        self.columns_to_impute = columns_to_impute
        self.imputation_strategy = imputation_strategy

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        imputer = Imputer(inputCols=self.columns_to_impute,
                        outputCols=["{}_imputed".format(c) for c in self.columns_to_impute]
                        ).setStrategy(self.imputation_strategy)

        df_imputed = imputer.fit(df).transform(df)
        df_imputed = df_imputed.drop(*self.columns_to_impute)
        
        # Rename back to original columns
        for c in self.columns_to_impute:
            df_imputed = df_imputed.withColumnRenamed("{}_imputed".format(c), c)

        return df_imputed
