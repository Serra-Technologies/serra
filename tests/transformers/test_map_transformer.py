from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from tests.base_test import SparkETLTestCase

from serra.transformers.map_transformer import (
    MapTransformer
)

class MapTransformerTest(SparkETLTestCase):
    def test_map_transformer(self):

        df = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234, location='CA'),
                Row(person='Alan', id=4321, location='TX')
            ]
        )

        result = MapTransformer(output_column="location_full",
                                mapping_dictionary={'CA':'California', 'TX':'Texas'},
                                input_column="location").transform(df)

        expected_schema = StructType(
            [
                StructField('person', StringType()),
                StructField('id', LongType()),
                StructField('location', StringType()),
                StructField('location_full', StringType())
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(person='Albert', id=1234, location='CA', location_full='California'),
                Row(person='Alan', id=4321, location='TX', location_full='Texas' )
            ],
            expected_schema
        )
        
        self.assertEqual(expected.collect(), result.collect())