from pyspark.sql import SparkSession
from serra.profile import SerraProfile


def misc_config(spark_builder):
    return spark_builder.config("spark.sql.debug.maxToStringFields", 100)

def add_s3_config(spark_builder, serra_profile: SerraProfile):
     s3_access_key = serra_profile.aws_access_key_id
     s3_secret_key = serra_profile.aws_secret_access_key

     builder = (
        spark_builder.config('spark.hadoop.fs.s3a.access.key', s3_access_key)
        .config('spark.hadoop.fs.s3a.secret.key', s3_secret_key)
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
     )
     return builder

def set_jar_packages(spark_builder):
    packages = [
        # for s3
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.530",
        "org.apache.hadoop:hadoop-common:3.3.4",
        # For bigquery
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2",
        # For mongo db
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"
        ]  
    spark_builder = spark_builder.config('spark.jars.packages', ','.join(packages))
    return spark_builder

def get_or_create_spark_session(serra_profile: SerraProfile):
    # TODO: Take a look for spark session conf: https://engineeringfordatascience.com/posts/pyspark_unit_testing_with_pytest/
    builder = SparkSession.builder
    builder = set_jar_packages(builder)
    builder = add_s3_config(builder, serra_profile)
    builder = misc_config(builder)
    return builder.getOrCreate()
