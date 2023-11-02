import snowflake.connector
from loguru import logger

from serra.utils import get_local_serra_profile
from serra.exceptions import SerraRunException
from serra.writers import Writer

class SnowflakeWriter(Writer):
    """
    A writer to write data from a Spark DataFrame to a Snowflake table.

    :param config: A dictionary containing the configuration for the writer.
                   It should have the following keys:
                   - 'type': The type of operation to perform. Possible values are 'create' or 'insert'.
                   - 'warehouse': The Snowflake warehouse to use for the connection.
                   - 'database': The name of the Snowflake database to write to.
                   - 'schema': The name of the Snowflake schema to write to.
                   - 'table': The name of the Snowflake table to write to.
    """

    def __init__(self, warehouse, database, schema, table, type):
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.table = table
        self.type = type
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
    
    def write(self, spark_df):
        """
        Write data from a Spark DataFrame to a Snowflake table.

        :param spark_df: The Spark DataFrame to be written to the Snowflake table.
        """
        pandas_df = spark_df.toPandas()

        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )

        ctx = conn.cursor()

        data = pandas_df.values.tolist()
        placeholders = ','.join(['%s']*len(pandas_df.columns))

        # If creating entirely new table
        if self.type == 'create': 
            create_table_sql = f"CREATE OR REPLACE TABLE {self.table} ("

            # Get schema
            for column in pandas_df.columns:
                data_type = pandas_df[column].dtype
                if data_type == 'int64':
                    data_type = 'NUMBER'
                elif data_type == 'float64':
                    data_type = 'FLOAT'
                elif data_type == 'bool':
                    data_type = 'BOOLEAN'
                elif data_type == 'object':
                    max_length = pandas_df[column].apply(lambda x: len(str(x))).max()
                    if max_length <= 16777215:
                        data_type = f'VARCHAR({max_length})'
                    else:
                        data_type = 'TEXT'
                elif data_type == 'datetime64[ns]':
                    data_type = 'TIMESTAMP_NTZ'
                elif data_type == 'timedelta64[ns]':
                    data_type = 'TIME'
                
                if ' ' in column:
                    column = f'"{column}"'
                
                create_table_sql += f"{column} {data_type}, "

            create_table_sql = create_table_sql.rstrip(', ') + ")"
            ctx.execute(create_table_sql)

        insert_sql = f"INSERT INTO {self.table} VALUES ({placeholders})"

        try:
            ctx.executemany(insert_sql, data)

            conn.commit()
            if self.type == 'create':
                logger.info("Table successfully created with data.")
            else:
                logger.info("Data successfully inserted into the target table.")
        except Exception as e:
            conn.rollback()
            raise SerraRunException(f"Error: Writer Failed. {e}")
