from serra.python.base import PythonTransformer
import math, random, numpy, re

class FormulaTransformer(PythonTransformer):
    """
    Allows business users to write their own math transforms
    """

    def __init__(self, text, output_column):
        self.text = text
        self.output_column = output_column

    def transform(self, df):
        # Input: min(random(facility))
        # Output: Eval min(random.randint(df['facility']))
            # Use regex to map business user functions to valid Python functions (if not in map, check if valid and use same one), add df.args

        math_map = {
            "random": "random.randint",
            **{func: f"numpy.{func}" for func in ['sqrt', 'exp', 'floor', 'pow', 'ceil', 'fabs']} # Unpack dictionary of math module function mappings
            # min, max are valid
        }

        def replace_func(match):
            # Input: min(sqrt(col1))
            # Output: min(math.sqrt(col1))

            if math_map.get(match.group(1)) is not None: # Group 1: Function, Group 2: (
                return math_map.get(match.group(1)) + match.group(2)
            else: # Valid function, todo implement valid function check else eval might fail
                return match.group(0)
            
        
        valid_function = re.sub(r"(\w+)(\()", replace_func, self.text)
        valid_function_args = re.sub(r"\(([^()]+)\)", r"(df['\1'])", valid_function)

        df[self.output_column] = eval(valid_function_args)
        return df


        




