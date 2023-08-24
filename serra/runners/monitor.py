# A helper class to keep track of the run and provide results

def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))

def get_json_rep(df, rows=20):
    df_head = df.limit(20)

    # Get column names
    columns = df.columns

    # Get schema information
    schema_info = [field.jsonValue() for field in df.schema.fields]

    # Convert each row to a dictionary and collect them as a list
    json_data = []
    for row in df_head.collect():
        row_dict = {col: value for col, value in zip(columns, row)}
        json_data.append(row_dict)

    # Create a dictionary for the JSON output
    output_dict = {
        "columns": columns,
        "schema": schema_info,
        "data": json_data
    }
    return output_dict


class Monitor:
    def __init__(self):
        self.step_to_df = {}
        self.step_to_json_rep = {}
        pass

    def to_dict(self):
        # Provide a json representation of the run
        ret = {
            "string_rep": self.step_to_df,
            "json_rep": self.step_to_json_rep
        }
        return ret
    
    
    def log_job_step(self,step_name, df):
        self.step_to_df[step_name] = getShowString(df)
        self.step_to_json_rep[step_name] = get_json_rep(df)
