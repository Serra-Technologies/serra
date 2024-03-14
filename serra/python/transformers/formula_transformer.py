from serra.python.base import PythonTransformer
import pandas as pd
import numpy as np

class FormulaTransformer(PythonTransformer):
    """
    Allows business users to write their own math transforms
    """

    def __init__(self, new_column_name, if_condition, then_statement, else_statement):
        self.new_column_name = new_column_name
        self.if_condition = if_condition
        self.then_statement = then_statement
        self.else_statement = else_statement

    def transform(self, df):
        def is_valid_eval_output(output):
            return type(output) == pd.Series or type(output) == bool or output == None


        # Evaluate expressions and ensure they are of the correct type
        eval_if_condition = df.eval(self.if_condition)
        eval_then_condition = df.eval(self.then_statement)
        eval_else_condition = df.eval(self.else_statement)
        # assert is_valid_eval_output(eval_if_condition)
        # assert is_valid_eval_output(eval_then_condition)
        # assert is_valid_eval_output(eval_else_condition)

        out = np.where(eval_if_condition, eval_then_condition, eval_else_condition)
        df[self.new_column_name] = out
        return df




