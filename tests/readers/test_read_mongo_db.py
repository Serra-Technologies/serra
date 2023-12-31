from pyspark.sql import Row
from unittest.mock import patch
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from tests.base_test import SparkETLTestCase

from serra.readers.mongo_db_reader import MongoDBReader

class MongoDbReaderTest(SparkETLTestCase):
    def test_mongo_db_reader(self):

        reader = MongoDBReader(
            username="serra_test_user",
            password="*****",
            database="test_database",
            collection="test_collection",
            cluster_ip_and_options="cluster0.ijbg2ly.mongodb.net/?retryWrites=true&w=majority"
        )
        reader.spark = self.spark
        mock_df = self.spark.createDataFrame(
            [
                ("ObjectId('5f50c31e1c4ae837d5a83a36')", 'john.doe@example.com', 'John Doe'),
                ("ObjectId('5f50c3201c4ae837d5a83a37')", 'jane.smith@example.com', 'Jane Smith')
            ],
            ['_id', 'email', 'name']
        )

        with patch('pyspark.sql.readwriter.DataFrameReader.load', return_value=mock_df):
            # Now when you call the read method, it will return the mock DataFrame
            result = reader.read()

        expected_schema = StructType(
            [
                StructField('_id', StringType()),
                StructField('email', StringType()),
                StructField('name', StringType())
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(_id="ObjectId('5f50c31e1c4ae837d5a83a36')", email='john.doe@example.com', name='John Doe'),
                Row(_id="ObjectId('5f50c3201c4ae837d5a83a37')", email='jane.smith@example.com', name='Jane Smith')
            ],
            expected_schema
        )
        
        self.assertEqual(result.collect(), expected.collect())