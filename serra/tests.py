from pyspark.sql.functions import when, count, isnull, col

from serra.exceptions import SerraRunException

def nulls_test(df):
    has_nulls = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    if has_nulls:
        raise SerraRunException("Null values were found in the DataFrame")
    
def duplicates_test(df):
    if df.count() > df.dropDuplicates(df.columns).count():
        raise SerraRunException('Data has duplicates')
    
def check_dates(df):
    # Define the timestamp columns
    # Define the timestamp column
    timestamp_column = "time"

    # Apply the date comparison to the specified column
    df = df.withColumn(timestamp_column, col(timestamp_column) > "2020-01-01T00:00:00Z")
    # Use 'all' to check if all columns satisfy the condition
    # Count the rows where the condition is satisfied
    count_true = df.filter(col(timestamp_column)).count()

    # Compare the count with the total number of rows
    if count_true != df.count():
        raise SerraRunException('Data has dates outside of valid range')

