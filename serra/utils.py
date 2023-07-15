import pkg_resources
from pyspark.sql import SparkSession
from serra.config import CONFIG_WORKSPACE_NAME

def write_to_file(filename, content):
    try:
        with open(filename, 'w') as file:
            file.write(content)
        print(f"Successfully wrote to {filename}.")
    except IOError:
        print(f"Error writing to {filename}.")

def import_class(cl):
    d = cl.rfind(".")
    classname = cl[d + 1 : len(cl)]
    m = __import__(cl[0:d], globals(), locals(), [classname])
    return getattr(m, classname)

def get_or_create_spark_session_with_name(app_name):
    spark = SparkSession.builder.appName(app_name).config().enableHiveSupport().getOrCreate()
    return spark

def get_or_create_spark_session():
    # These installations are only really necessary when running locally
    packages = [
        f'org.apache.hadoop:hadoop-aws:3.3.1',
        'com.google.guava:guava:30.1.1-jre',
        'org.apache.httpcomponents:httpcore:4.4.14', 
        'com.google.inject:guice:4.2.2', 
        'com.google.inject.extensions:guice-servlet:4.2.2'
        ]   
    spark = (SparkSession.builder
        .config('spark.jars.packages', ','.join(packages))
        .config('spark.hadoop.fs.s3a.access.key', 'AKIA3TOV3GZZHAH4MPAE')
        .config('spark.hadoop.fs.s3a.secret.key', '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')
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .getOrCreate())
    return spark

def get_path_to_user_configs_folder():
    return pkg_resources.resource_filename('serra', CONFIG_WORKSPACE_NAME)