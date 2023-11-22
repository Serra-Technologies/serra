from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from tests.base_test import SparkETLTestCase

from serra.transformers.rename_column_transformer import (
    RenameColumnTransformer
)

class RenameColumnTransformerTest(SparkETLTestCase):
    def test_rename_column_transformer(self):

        df = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234),
                Row(person='Alan', id=4321)
            ]
        )

        result = RenameColumnTransformer(old_name="person", new_name="name").transform(df)

        expected_schema = StructType(
            [
                StructField('id', LongType()),
                StructField('name', StringType())
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(id=1234, name='Albert'),
                Row(id=4321, name='Alan')
            ],
            expected_schema
        )
        
        self.assertEqual(expected.collect(), result.collect())