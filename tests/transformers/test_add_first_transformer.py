from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

from tests.base_test import SparkETLTestCase

from serra.transformers.add_first_transformer import (
    AddFirstTransformer
)

from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
import datetime

class AddFirstTransformerTest(SparkETLTestCase):

    def test_medium_and_advertising_name(self):
        data = [
            (datetime.datetime(2023, 1, 1, 9, 0), "id1","cpc",None),
            (datetime.datetime(2023, 1, 1, 10, 0), "id2","display",None),
            (datetime.datetime(2023, 1, 1, 11, 0), "id3",None,"some_partner")
        ]
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("APP_ADVERTISING_PARTNER_NAME", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        result = AddFirstTransformer(
            output_column="first_app_session",
            condition="medium IN ('cpc', 'display') OR APP_ADVERTISING_PARTNER_NAME IS NOT NULL"
        ).transform(df)

        # What we expect

        expected_schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("APP_ADVERTISING_PARTNER_NAME", StringType(), True),
            StructField("first_app_session", BooleanType(), True)
        ])
        expected = self.spark.createDataFrame(
            [
                Row(event_time=datetime.datetime(2023, 1, 1, 9, 0),
                    final_amplitude_id="id1",
                    medium="cpc",
                    APP_ADVERTISING_PARTNER_NAME=None,
                    first_app_session=True
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 10, 0),
                    final_amplitude_id="id2",
                    medium="display",
                    APP_ADVERTISING_PARTNER_NAME=None,
                    first_app_session=True
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 11, 0),
                    final_amplitude_id="id3",
                    medium=None,
                    APP_ADVERTISING_PARTNER_NAME="some_partner",
                    first_app_session=True
                    )
            ],
            expected_schema
        )

        # Comparison
        self.assertEqual(result.collect(), expected.collect())

    def test_second_touch_is_cpc(self):
        data = [
            (datetime.datetime(2023, 1, 1, 9, 0), "id1",None,None),
            (datetime.datetime(2023, 1, 1, 10, 0), "id1","cpc",None)
        ]
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("APP_ADVERTISING_PARTNER_NAME", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        result = AddFirstTransformer(
            output_column="first_app_session",
            condition="medium IN ('cpc', 'display') OR APP_ADVERTISING_PARTNER_NAME IS NOT NULL"
        ).transform(df)

        # What we expect

        expected_schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("APP_ADVERTISING_PARTNER_NAME", StringType(), True),
            StructField("first_app_session", BooleanType(), True)
        ])
        expected = self.spark.createDataFrame(
            [
                Row(event_time=datetime.datetime(2023, 1, 1, 9, 0),
                    final_amplitude_id="id1",
                    medium=None,
                    APP_ADVERTISING_PARTNER_NAME=None,
                    first_app_session=False
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 10, 0),
                    final_amplitude_id="id1",
                    medium="cpc",
                    APP_ADVERTISING_PARTNER_NAME=None,
                    first_app_session=True
                    )
            ],
            expected_schema
        )

        # Comparison
        self.assertEqual(result.collect(), expected.collect())

    def test_first_session_web_second_app(self):
        data = [
            (datetime.datetime(2023, 1, 1, 9, 0), "id1","web"),
            (datetime.datetime(2023, 1, 1, 10, 0), "id1","iOS")
        ]
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("platform", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        result = AddFirstTransformer(
            output_column="first_app_session",
            condition="platform IN ('iOS')"
        ).transform(df)

        # What we expect

        expected_schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("first_app_session", BooleanType(), True)
        ])
        expected = self.spark.createDataFrame(
            [
                Row(event_time=datetime.datetime(2023, 1, 1, 9, 0),
                    final_amplitude_id="id1",
                    platform="web",
                    first_app_session=False
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 10, 0),
                    final_amplitude_id="id1",
                    platform="iOS",
                    first_app_session=True
                    )
            ],
            expected_schema
        )

        # Comparison
        self.assertEqual(result.collect(), expected.collect())

    def test_web_1_ios_1_ios1(self):
        data = [
            (datetime.datetime(2023, 1, 1, 9, 0), "id1", "Web"),
            (datetime.datetime(2023, 1, 1, 10, 0), "id1","iOS"),
            (datetime.datetime(2023, 1, 1, 10, 0), "id1","iOS")
        ]
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("platform", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        result = AddFirstTransformer(
            output_column="first_app_session",
            condition="platform IN ('iOS')"
        ).transform(df)

        # What we expect

        expected_schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("final_amplitude_id", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("first_app_session", BooleanType(), True)
        ])
        expected = self.spark.createDataFrame(
            [
                Row(event_time=datetime.datetime(2023, 1, 1, 9, 0),
                    final_amplitude_id="id1",
                    platform="Web",
                    first_app_session=False
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 10, 0),
                    final_amplitude_id="id1",
                    platform="iOS",
                    first_app_session=True
                    ),
                Row(event_time=datetime.datetime(2023, 1, 1, 10, 0),
                    final_amplitude_id="id1",
                    platform="iOS",
                    first_app_session=False
                    )
            ],
            expected_schema
        )

        # Comparison
        self.assertEqual(result.collect(), expected.collect())