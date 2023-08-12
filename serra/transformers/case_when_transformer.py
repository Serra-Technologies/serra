from pyspark.sql.functions import col,when 

from serra.transformers.transformer import Transformer

class CaseWhenTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.output_col = self.config.get("output_col")
        self.input_col = self.config.get("input_col")
        self.conditions = self.config.get('conditions')
        self.type = self.config.get('type')
        self.is_col = self.config.get('is_col')
        self.otherwise_val = config.get('otherwise_val')

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
        comparison_func = type_dict.get(self.type)

        if comparison_func is None:
            raise ValueError(f"Unsupported comparison type: {self.type}")
        print('###########', self.conditions[0][0])
        # Create the 'when' expression based on the provided conditions and type
        if self.is_col is None:
            case_expr = when(comparison_func(df[self.input_col], self.conditions[0][0]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_col], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(None)

            return df.withColumn(self.output_col, case_expr)
        else:
            case_expr = when(comparison_func(df[self.input_col], df[self.conditions[0][0]]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_col], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(self.otherwise_val)

            return df.withColumn(self.output_col, case_expr)
    
    def parse_result_value(self, result_value):
        if isinstance(result_value,str):
            if 'col:' in result_value:
                return col(result_value[4:])
        else:
            return(result_value)
