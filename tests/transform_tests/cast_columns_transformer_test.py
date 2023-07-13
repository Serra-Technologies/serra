from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.cast_columns_transformer import (
    CastColumnTransformer
)

class CastColumnsTransformerTest(SQLTestCase):
    def test_cast_columns_transformer(self):

        df = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', amount=5302.34),
                Row(person='Alan', amount=4321.3)
            ]
        )

        config = {
            'cast_columns': {
                'amount_int': ['amount','integer']
            }
        }

        result = CastColumnTransformer(config).transform(df)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('amount', DoubleType()),
                StructField('amount_int', IntegerType())
            ]
        )
        expected = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', amount=5302.34, amount_int=5302),
                Row(person='Alan', amount=4321.3, amount_int=4321)
            ],
            expected_schema
        )
        
        self.assertDataFrameEqual(expected, result)