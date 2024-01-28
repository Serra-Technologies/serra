from loguru import logger

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

    def __init__(self, warehouse, database, schema, table, host, user, password, mode):
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.table = table
        self.host = host
        self.user = user
        self.password = password
        self.mode = mode
    
    def write(self, df):
        """
        Write data from a Spark DataFrame to a Snowflake table.
        :param df: The Spark DataFrame to be written to the Snowflake table.
        """
        assert self.mode in ['append', 'overwrite', 'error', 'ignore']

        (df.write
        .format("snowflake") 
        .option("host", self.host)
        .option("user", self.user)
        .option("password", self.password)
        .option("sfWarehouse", self.warehouse)
        .option("database", self.database)
        .option("schema", self.schema)
        .option("dbtable", self.table) 
        .option("autopushdown", "off")
        .mode(self.mode) 
        .save()
        )
