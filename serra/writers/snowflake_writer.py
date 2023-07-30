import snowflake.connector
from serra.profile import get_serra_profile
from loguru import logger
from serra.exceptions import SerraRunException

class SnowflakeWriter():
    def __init__(self, config):
        self.config = config
        self.type = self.config.get('type')
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
        return [self.config.get('input_block')]
    
    def write(self, spark_df):
        pandas_df = spark_df.toPandas()

        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.config.get('warehouse'),
            database=self.config.get('database'),
            schema=self.config.get('schema')
        )

        ctx = conn.cursor()

        data = pandas_df.values.tolist()
        placeholders = ','.join(['%s']*len(pandas_df.columns))

        # If creating entirely new table
        if self.type == 'create': 
            create_table_sql = f"CREATE TABLE {self.config.get('table')} ("

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

        insert_sql = f"INSERT INTO {self.config.get('table')} VALUES ({placeholders})"

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
