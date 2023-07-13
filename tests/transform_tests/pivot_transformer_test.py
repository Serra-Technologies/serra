from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.pivot_transformer import (
    PivotTransformer
)

class PivotTransformerTest(SQLTestCase):
    def test_pivot_column_transformer(self):

        df = self.sqlCtx.createDataFrame(
            [
                Row(streaming_service='Netflix', subscriber_count=12000, region='CA', country='US'),
                Row(streaming_service='Disney', subscriber_count=2000, region='MA', country='US'),
                Row(streaming_service='Netflix', subscriber_count=50000, region='TX', country='US'),
                Row(streaming_service='Disney', subscriber_count=100, region='AK', country='US'),
                Row(streaming_service='Netflix', subscriber_count=1500, region='A', country='China'),
                Row(streaming_service='Disney', subscriber_count=1000, region='B', country='China'),
                Row(streaming_service='Netflix', subscriber_count=20000, region='C', country='China'),
                Row(streaming_service='Disney', subscriber_count=12000, region='D', country='China'),
            ]
        )

        config = {
            'row_level': 'streaming_service',
            'column_level': 'country',
            'sum_col': 'subscriber_count'
        }

        result = PivotTransformer(config).transform(df)

        expected_schema = StructType(
            [
                StructField('streaming_service', StringType()),
                StructField('China', LongType()),
                StructField('US', LongType())
            ]
        )
        expected = self.sqlCtx.createDataFrame(
            [
                Row(streaming_service='Disney', China=13000, US=2100),
                Row(streaming_service='Netflix', China=21500, US=62000)
            ],
            expected_schema
        )
        
        self.assertDataFrameEqual(expected, result)