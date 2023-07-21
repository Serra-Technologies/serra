from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from tests.base_test import SparkETLTestCase

from serra.transformers.join_transformer import (
    JoinTransformer
)

class JoinTransformerTest(SparkETLTestCase):
    def test_join_transformer(self):

        df = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234),
                Row(person='Alan', id=4321)
            ]
        )
        df2 = self.spark.createDataFrame(
            [
                Row(privacy_name='Test1', id=1234),
                Row(privacy_name='Test2', id=4321)
            ]
        )

        config = {
            'join_type':'inner',
            'join_on': {
                "some": "id",
                "thing": "id"
            }
        }

        result = JoinTransformer(config).transform(df,df2)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('id', LongType()),
                StructField('privacy_name', StringType())
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234, privacy_name='Test1'),
                Row(person='Alan', id=4321, privacy_name='Test2')
            ],
            expected_schema
        )

        expected.show()
        result.show()
        
        self.assertEqual(result.collect(), expected.collect())