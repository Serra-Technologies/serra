from pyspark.sql.functions import when, count, isnull
from serra.exceptions import SerraRunException

def nulls_test(df):
    has_nulls = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    if has_nulls:
        raise SerraRunException("Null values were found in the DataFrame")
    
def duplicates_test(df):
    if df.count() > df.dropDuplicates(df.columns).count():
        raise SerraRunException('Data has duplicates')