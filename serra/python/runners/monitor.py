# Helper functions
import numpy as np
def get_json_rep_pandas(df, rows=20):
    df_head = df.head(rows).replace({np.nan: None, float('nan'): None})

    # Get column names
    columns = list(df.columns)

    # Get schema information
    schema_info = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]

    # Convert each row to a dictionary and collect them as a list
    json_data = df_head.to_dict('records')

    # Create a dictionary for the JSON output
    output_dict = {
        "columns": columns,
        "schema": schema_info,
        "data": json_data
    }
    return output_dict

# Output helpers
class Monitor:
    # PythonMonitor
    def __init__(self):
        self.step_to_df = {}
        self.step_to_json_rep = {}

    def to_dict(self):
        # Provide a json representation of the run
        ret = {
            "json_rep": self.step_to_json_rep
        }
        return ret
    
    
    def log_job_step(self,step_name, df):
        self.step_to_json_rep[step_name] = get_json_rep_pandas(df)