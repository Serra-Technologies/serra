from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.read.table("demo.sales_by_store")
df.show(5)

# install databricks-connect
# configure ~/databricks file
# set up credentials