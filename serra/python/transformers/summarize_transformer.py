from serra.python.base import PythonTransformer
class SummarizeTransformer(PythonTransformer):
    """
    This class is a transformer that takes in a dataframe and returns a dataframe with the summary statistics
    of the input dataframe. The summary statistics are defined by the user and are passed in as a list of dictionaries.
    Each dictionary contains the column name and the summary statistic to be calculated. The summary statistics are
    calculated using the pandas library.
    summary_transforms: list of dictionaries
        format: [{"column": "column_name", "function": "summary_statistic"}, ...]

    the summary_statistic can be one of: "Sum", "Mean", "Min", "Max", "Concatenate"

    summary_transforms example: 
    [
            {"column": "school", 'summary_function': "Group By"},
            {"column": "points", 'summary_function': "Sum"},
            {"column": "points", 'summary_function': "Mean"},
            {"column": "points", 'summary_function': "Min"},
            {"column": "points", 'summary_function': "Max"},
            {"column": "Name", 'summary_function': "Concatenate"}
    ]
    """

    def __init__(self, summary_transforms):
        self.summary_transforms = summary_transforms

    def transform(self, df):
        example_config = self.summary_transforms

        def concatenate(x):
            return ", ".join(x.values)

        convert_selected_function_to_function = {
            "Sum": "sum",
            "Mean": "mean",
            "Min": "min",
            "Max": "max",
            "Concatenate": concatenate
        }

        # Configure the input for the pandas group by
        group_by_columns = []
        for config in example_config:
            if config['summary_function'] == "Group By":
                group_by_columns.append(config['column'])

        # Configure the input for aggregation for pandas
        agg_input = {}
        for config in example_config:
            if config['summary_function'] == "Group By":
                continue
            
            # Get converted function
            func = convert_selected_function_to_function[config['summary_function']]
            
            if config['column'] in agg_input:
                agg_input[config['column']].append(func)
            else:
                agg_input[config['column']] = [func]

        # TODO: Verify it's being used properly
        number_functions = ["Sum", "Mean", "Min", "Max"]
        string_functions = ["Concatenate"]

        another_group = df.groupby(group_by_columns)
        final = another_group.agg(agg_input)

        # Concatenate 
        final.columns = ["_".join(a) for a in final.columns.to_flat_index()]

        # Convert pandas dataframe with multiindex to single index
        final.reset_index(inplace=True)

        return final
