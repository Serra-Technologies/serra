from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.join_transformer import (
    JoinTransformer
)

class JoinTransformerTest(SQLTestCase):
    def test_join_transformer(self):

        df = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', id=1234),
                Row(person='Alan', id=4321)
            ]
        )
        df2 = self.sqlCtx.createDataFrame(
            [
                Row(privacy_name='Test1', id=1234),
                Row(privacy_name='Test2', id=4321)
            ]
        )

        config = {
            'join_type':'inner',
            'matching_col': 'id'
        }

        result = JoinTransformer(config).transform(df,df2)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('id', LongType()),
                StructField('privacy_name', StringType())
            ]
        )
        expected = self.sqlCtx.createDataFrame(
            [
                Row(person='Alan', id=4321, privacy_name='Test2'),
                Row(person='Albert', id=1234, privacy_name='Test1'),
            ],
            expected_schema
        )

        expected.show()
        result.show()
        
        self.assertDataFrameEqual(result, expected)