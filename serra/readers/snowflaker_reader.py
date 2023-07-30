import snowflake.connector
from serra.profile import get_serra_profile
from serra.utils import get_or_create_spark_session
import pandas as pd

class SnowflakeReader():
    def __init__(self, config):
        self.config = config
        self.snowflake_account = get_serra_profile().snowflake_account
    
    @property
    def user(self):
        return self.snowflake_account.get("USER")
    
    @property
    def password(self):
        return self.snowflake_account.get("PASSWORD")
    
    @property
    def account(self):
        return self.snowflake_account.get("ACCOUNT")

    @property
    def dependencies(self):
        return []
    
    def read(self):
        # Establish snowflake connection
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.config.get('warehouse'),
            database=self.config.get('database'),
            schema=self.config.get('schema')
            )

        ctx = conn.cursor()
        ctx.execute(f"select * from {self.config.get('table')}")

        results = ctx.fetchall()
        column_names = [column[0] for column in ctx.description]
        df = pd.DataFrame(results, columns=column_names)

        spark = get_or_create_spark_session()
        spark_df = spark.createDataFrame(df)
        return spark_df
