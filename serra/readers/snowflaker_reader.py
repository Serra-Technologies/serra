import pandas as pd
import snowflake.connector

from serra.utils import get_local_serra_profile
from serra.readers import Reader

class SnowflakeReader(Reader):
    """
    A reader to read data from Snowflake into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'warehouse': The Snowflake warehouse to use for the connection.
                   - 'database': The Snowflake database to use for the connection.
                   - 'schema': The Snowflake schema to use for the connection.
                   - 'table': The name of the table to be read from Snowflake.
    """

    def __init__(self, warehouse, database, schema, table):
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.table = table
        self.serra_profile = get_local_serra_profile()
    
    @property
    def snowflake_account(self):
        return self.serra_profile.snowflake_account

    @property
    def user(self):
        return self.snowflake_account.get("USER")
    
    @property
    def password(self):
        return self.snowflake_account.get("PASSWORD")
    
    @property
    def account(self):
        return self.snowflake_account.get("ACCOUNT")
    
    def read(self):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
            )

        ctx = conn.cursor()
        ctx.execute(f"select * from {self.table}")

        results = ctx.fetchall()
        column_names = [column[0] for column in ctx.description]
        df = pd.DataFrame(results, columns=column_names)

        spark = self.spark
        spark_df = spark.createDataFrame(df)
        return spark_df

    def read_with_spark(self, spark):
        self.spark = spark
        return self.read()