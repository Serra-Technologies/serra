from pyspark.sql.functions import col,when 

from serra.transformers.transformer import Transformer

class CaseWhenTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param output_column: The name of the new output column to be added.
    :param input_column: The name of the input column.
    :param conditions: A list of conditions to evaluate. Each condition should be a tuple of (condition_column, operator, value).
    :param comparison_type: The type of comparison to use (e.g., 'equals', 'greater_than').
    :param is_column_condition: Boolean indicating whether the condition is based on a column value.
    :param otherwise_value: The value to use when none of the conditions are met.
    """

    def __init__(self, output_column, input_column, conditions, comparison_type, is_column_condition, otherwise_value):
        self.output_column = output_column
        self.input_column = input_column
        self.conditions = conditions
        self.comparison_type = comparison_type
        self.is_column_condition = is_column_condition
        self.otherwise_value = otherwise_value

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """
        print("Conditions:", self.conditions[0][0])
        # Mappings to indicate which comparison function to use
        type_dict = {
            '<=': lambda col, val: col <= val,
            '>=': lambda col, val: col >= val,
            '==': lambda col, val: col == val,
            'like': lambda col, val: col.like(val)
        }

        # Get the particular condition function based on the comparison type
        comparison_func = type_dict.get(self.comparison_type)

        if comparison_func is None:
            raise ValueError(f"Unsupported comparison type: {self.comparison_type}")
        print('###########', self.conditions[0][0])
        # Create the 'when' expression based on the provided conditions and type
        if self.is_column_condition is None:
            case_expr = when(comparison_func(df[self.input_column], self.conditions[0][0]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_column], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(None)

            return df.withColumn(self.output_column, case_expr)
        else:
            case_expr = when(comparison_func(df[self.input_column], df[self.conditions[0][0]]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_column], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(self.otherwise_value)

            return df.withColumn(self.output_column, case_expr)
    
    def parse_result_value(self, result_value):
        if isinstance(result_value,str) and 'col:' in result_value:
            return col(result_value[4:])
        else:
            return(result_value)
