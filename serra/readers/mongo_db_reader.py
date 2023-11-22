from serra.readers import Reader

class MongoDBReader(Reader):
    def __init__(self, username, password, database, collection, cluster_ip_and_options):
        self.username = username
        self.password = password
        self.database = database
        self.collection = collection
        self.cluster_ip_and_options = cluster_ip_and_options

    def read(self):
        spark = self.spark

        #TODO: connection.uri can either start iwth mongodb+srv or mongodb

        df = spark.read\
        .format("mongodb")\
        .option("connection.uri", f'mongodb+srv://{self.username}:{self.password}@{self.cluster_ip_and_options}')\
        .option("database", self.database)\
        .option("collection", self.collection)\
        .load()
        return df