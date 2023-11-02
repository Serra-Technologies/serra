from pyspark.sql.functions import col, when

from serra.transformers.transformer import Transformer

class MultipleCaseWhenTransformer(Transformer):
    """
    A transformer that adds a new column to the DataFrame based on conditional rules (similar to SQL's CASE WHEN).

    :param columns_and_conditions: A list of tuples representing the conditions and their corresponding results.
                                   Each tuple should be in the format (condition_value, result_value).
                                   The condition_value can be a specific value or a pattern for LIKE comparisons.
                                   The result_value will be assigned to the output_col if the condition is met.
    :param input_column: The name of the column containing the values to be evaluated.
    :param output_col: The name of the new column to be added with the results of the conditions.
    :param type: The type of comparison to be used. It can be either '==' for equality comparison or 'like' for pattern matching using the LIKE operator.
    """

    def __init__(self, columns_and_conditions, input_column, output_col, type):
        self.columns_and_conditions = columns_and_conditions
        self.input_column = input_column
        self.output_col = output_col
        self.type = type

    def transform(self, df):
        """
        Add a new column with the results of the conditions to the DataFrame.

        :param df: The input DataFrame.
        :return: A new DataFrame with an additional column containing the results of the conditions.
        """
        # Mappings to indicate which comparison function to use
        type_dict = {
            '<=': lambda col, val: col <= val,
            '>=': lambda col, val: col >= val,
            '==': lambda col, val: col == val,
            'like': lambda col, val: col.like(val)
        }

        # Get the particular condition function based on the comparison type
        comparison_func = type_dict.get(self.type)
        output_cols = list(self.columns_and_conditions.keys())
        conditions = list(self.columns_and_conditions.values())

        if comparison_func is None:
            raise ValueError(f"Unsupported comparison type: {self.type}")
        
        for i in range(len(output_cols)):
            if len(conditions[i]) == 2:
                case_expr = when(comparison_func(df[self.input_column[0]], conditions[i][0]), self.parse_result_value(conditions[i][1]))
            else: 
                case_expr = when(comparison_func(df[self.input_column[0]], conditions[i][0]) | comparison_func(df[self.input_column[0]], conditions[i][2]), self.parse_result_value(conditions[i][1]))
            case_expr = case_expr.otherwise(None)
            df = df.withColumn(output_cols[i], case_expr)

        return df
    
    def parse_result_value(self, result_value):
        if 'col:' in result_value:
            return col(result_value[4:])
        else:
            return(result_value)
