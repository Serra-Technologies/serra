from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from sparktestingbase.sqltestcase import SQLTestCase

from serra.transformers.map_transformer import (
    MapTransformer
)

class MapTransformerTest(SQLTestCase):
    def test_map_transformer(self):

        df = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', id=1234, location='CA'),
                Row(person='Alan', id=4321, location='TX')
            ]
        )

        config = {
            'name':'location_full',
            'map_dict': {'CA':'California', 'TX':'Texas'},
            'col_key':'location'
        }

        result = MapTransformer(config).transform(df)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('id', LongType()),
                StructField('location', StringType()),
                StructField('location_full', StringType())
            ]
        )
        expected = self.sqlCtx.createDataFrame(
            [
                Row(person='Albert', id=1234, location='CA', location_full='California'),
                Row(person='Alan', id=4321, location='TX', location_full='Texas' )
            ],
            expected_schema
        )
        
        self.assertDataFrameEqual(expected, result)