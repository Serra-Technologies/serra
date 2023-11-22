from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

from tests.base_test import SparkETLTestCase

from serra.transformers.cast_columns_transformer import (
    CastColumnsTransformer
)

class CastColumnsTransformerTest(SparkETLTestCase):
    def test_cast_columns_transformer(self):

        df = self.spark.createDataFrame(
            [
                Row(person='Albert', amount=5302.34),
                Row(person='Alan', amount=4321.3)
            ]
        )

        cast_columns = {
            'amount_int': ['amount','integer']
        }

        result = CastColumnsTransformer(cast_columns).transform(df)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('amount', DoubleType()),
                StructField('amount_int', IntegerType())
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(person='Albert', amount=5302.34, amount_int=5302),
                Row(person='Alan', amount=4321.3, amount_int=4321)
            ],
            expected_schema
        )
        
        self.assertEqual(result.collect(), expected.collect())