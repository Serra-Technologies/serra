import unittest
from pyspark.sql import SparkSession
from serra.spark import set_jar_packages


# https://towardsdatascience.com/the-elephant-in-the-room-how-to-write-pyspark-unit-tests-a5073acabc34

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark_session = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     )
        spark_session = set_jar_packages(spark_session)
        spark_session = spark_session.getOrCreate()
        cls.spark: SparkSession = spark_session

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()