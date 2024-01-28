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

    def __init__(self, warehouse, database, schema, table, user, password, host):
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.table = table
        self.user = user
        self.password = password
        self.host = host
    
    def read(self):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        snowflake_table = (self.spark.read
        .format("snowflake")
        .option("host", self.host)
        .option("user", self.user)
        .option("password", self.password)
        .option("sfWarehouse", self.warehouse)
        .option("database", self.database)
        .option("schema", self.schema) # Optional - will use default schema "public" if not specified.
        .option("autopushdown", "off")
        .option("dbtable", self.table)
        .load()
        )
        return snowflake_table