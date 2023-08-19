# A helper class to keep track of the run and provide results

def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))

class Monitor:
    def __init__(self):
        self.step_to_df = {}
        pass

    def to_dict(self):
        # Provide a json representation of the run 
        return self.step_to_df
    
    def log_job_step(self,step_name, df):
        self.step_to_df[step_name] = getShowString(df)
