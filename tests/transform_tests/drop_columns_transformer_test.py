from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.drop_columns_transformer import (
    DropColumnTransformer
)

class DropColumnsTransformerTest(SQLTestCase):
    def test_drop_columns_transformer(self):

        df = self.sqlCtx.createDataFrame(
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
        expected = self.sqlCtx.createDataFrame(
            [
                Row(id=1234),
                Row(id=4321)
            ],
            expected_schema
        )
        
        self.assertDataFrameEqual(expected, result)