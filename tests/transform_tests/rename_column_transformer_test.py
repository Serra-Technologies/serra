from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.rename_column_transformer import (
    RenameColumn
)

class RenameColumnTransformerTest(SQLTestCase):
    def test_rename_column_transformer(self):

        df = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', id=1234),
                Row(person='Alan', id=4321)
            ]
        )

        config = {
            'old_name':'person',
            'new_name':'name'
        }

        result = RenameColumn(config).transform(df)

        expected_schema = StructType(
            [
                StructField('id', LongType()),
                StructField('name', StringType())
            ]
        )
        expected = self.sqlCtx.createDataFrame(
            [
                Row(id=1234, name='Albert'),
                Row(id=4321, name='Alan')
            ],
            expected_schema
        )
        
        self.assertDataFrameEqual(expected, result)