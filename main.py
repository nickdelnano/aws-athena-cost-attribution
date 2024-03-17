from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

@udf(returnType=StringType())
def get_query_execution(queryId):
    return queryId


spark = SparkSession.builder.appName("AthenaCloudtrail").getOrCreate()
df = spark.read.json("/home/iceberg/notebooks/notebooks/many_events.json")

queryIds = df.select(F.explode("Records").alias("record")).select("record.responseElements.queryExecutionId").distinct()
queryIds.select(udf_batch_get_data("queryExecutionId").alias("output")).show()
