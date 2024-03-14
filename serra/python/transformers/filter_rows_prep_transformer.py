from serra.python.base import PythonTransformer

# TODO: Add more operators >, <
class FilterRowsTransformer(PythonTransformer):
    """
    Drop rows that have nulls in specified column(s).

    :param column: The column to filter on
    :param operator: Comparison type (==, !=, >, <)
    :param value: The value to filter column on
    """

    def __init__(self, column, operator, value):
        self.column = column
        self.operator = operator
        self.value = value

    def transform(self, df):
        """
        First, get the column type and then convert the value to that specific type to ensure correctness while also keeping flexiblity on user input (ie. type = int, value = '130000', convert to 13000). This will help for later on where we can display operators as "equals" for strings and "=" for ints like Alteryx.
        Second, use the operator map to allow for flexible input on the operator side and then set the correct operator in code.
        Third, construct the filter query and use pandas query operator to execute it and return the filtered df. 
        """
        # Get column type, convert value
        column_type = str(df.dtypes[self.column])

        type_map = {
            "object": str,
            "int64": int,
            "float64": float
        }
        type_conversion = type_map.get(column_type)
    
        converted_value = type_conversion(self.value)
        if column_type == "object":
            converted_value = f"'{converted_value}'"

        # Get correct operator from dictionary, convert operator
        operator_map = {
            "Equals": "==",
            "Does not equal": "!="
        }
        operator = operator_map.get(self.operator)

        # Combine all things together into query, run and return filtered df
        query_str = f"`{self.column}` {operator} {converted_value}"

        return df.query(query_str)
