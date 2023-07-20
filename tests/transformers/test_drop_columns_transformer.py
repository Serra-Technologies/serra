from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from tests.base_test import SparkETLTestCase

from serra.transformers.drop_columns_transformer import (
    DropColumnTransformer
)

class DropColumnsTransformerTest(SparkETLTestCase):
    def test_drop_columns_transformer(self):

        df = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234),
                Row(person='Alan', id=4321)
            ]
        )

        config = {
            'drop_names': 'person'
        }

        result = DropColumnTransformer(config).transform(df)

        expected_schema = StructType(
            [
                StructField('id', LongType()),
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(id=1234),
                Row(id=4321)
            ],
            expected_schema
        )
        
        self.assertEqual(expected.collect(), result.collect())