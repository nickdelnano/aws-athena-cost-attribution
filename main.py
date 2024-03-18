import boto3
import re
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, struct, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, BooleanType, LongType
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

"""
TODO
* Cloudtrail is at least once, account for this. MERGE needs 1 row per pk.
* If a field is missing do not fail - e.g. planning time
* Include updates for State=Failed. They can be retried. Need to adjust MERGE condition, WHEN MATCHED AND t.state='FAILED' AND s.state='SUCCEEDED' THEN UPDATE SET *
* Add Error fields https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
"""

CATALOG_NAME = 'iceberg'
OUTPUT_DATABASE_NAME = 'ndelnano'
OUTPUT_TABLE_NAME = 'athena_attribution'
FULL_TABLE_NAME = f"{CATALOG_NAME}.{OUTPUT_DATABASE_NAME}.{OUTPUT_TABLE_NAME}"
WAREHOUSE = "s3://ndn-data-lake"


def get_query_execution(queryId):
    client = session.client("athena")
    response = client.get_query_execution(
        QueryExecutionId=queryId
    )

    state = response['QueryExecution']['Status']['State']

    # Only process queries that are completed
    # Include FAILED as these can still consume cost
    if state in ['QUEUED', 'RUNNING']:
        return

    bytes_in_a_tb = 1000000000000.0

    return Row(
        QueryExecutionId=response['QueryExecution']['QueryExecutionId'],
        QueryTxt=response['QueryExecution']['Query'],
        Database=response['QueryExecution']['QueryExecutionContext']['Database'],
        State=state,
        SubmissionTime=response['QueryExecution']['Status']['SubmissionDateTime'].replace(tzinfo=None),
        CompletionDateTime=response['QueryExecution']['Status']['CompletionDateTime'].replace(tzinfo=None),
        EngineExecutionTimeSeconds=response['QueryExecution']['Statistics']['EngineExecutionTimeInMillis'] / 1000.0,
        DataScannedInBytes=response['QueryExecution']['Statistics']['DataScannedInBytes'],
        ReusedPreviousResult=response['QueryExecution']['Statistics']['ResultReuseInformation']['ReusedPreviousResult'],
        Workgroup=response['QueryExecution']['WorkGroup'],
        SelectedEngineVersion=response['QueryExecution']['EngineVersion']['SelectedEngineVersion'],
        EffectiveEngineVersion=response['QueryExecution']['EngineVersion']['EffectiveEngineVersion'],
        DataScannedInTB=response['QueryExecution']['Statistics']['DataScannedInBytes'] / bytes_in_a_tb,
        DataScannedCostUSD=(response['QueryExecution']['Statistics']['DataScannedInBytes'] / bytes_in_a_tb) * 5,
    )


def extract_dbt_model(querytxt):
    matches = re.search(r'"node_id"\s*:\s*"([^"]+)"', querytxt)
    return matches.group(1) if matches else None


def table_exists(catalog, database, table):
    spark.sql(f"USE {catalog}")
    spark.sql(f"USE {database}")
    all_tables = spark.sql(
        "SHOW TABLES"
    ).collect()
    table_names = [row["tableName"] for row in all_tables]
    return table in table_names


session = boto3.Session(
    region_name="us-west-2",
)

spark = SparkSession.builder.appName("AthenaCloudtrail").getOrCreate()
spark.conf.set("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)

get_query_execution_udf_schema = StructType([
    StructField("QueryExecutionId", StringType()),
    StructField("QueryTxt", StringType()),
    StructField("Database", StringType()),
    StructField("State", StringType()),
    StructField("SubmissionTime", TimestampType()),
    StructField("CompletionDateTime", TimestampType()),
    StructField("EngineExecutionTimeSeconds", FloatType()),
    StructField("DataScannedInBytes", LongType()),
    StructField("ReusedPreviousResult", BooleanType()),
    StructField("Workgroup", StringType()),
    StructField("SelectedEngineVersion", StringType()),
    StructField("EffectiveEngineVersion", StringType()),
    StructField("DataScannedInTB", FloatType()),
    StructField("DataScannedCostUSD", FloatType()),
])

get_query_execution_udf = spark.udf.register("get_query_execution", get_query_execution, get_query_execution_udf_schema)
extract_dbt_model_udf = udf(extract_dbt_model, StringType())

# df = spark.read.json("/home/iceberg/notebooks/notebooks/many_events.json")
# queryIds = df.select(F.explode("Records").alias("record")).select("record.responseElements.queryExecutionId").distinct()

cloudtrail_df = spark.createDataFrame([
    Row(
        QueryExecutionId="1111",
        AccountId='1111', 
        UserIdentityArn='arn:aws:iam::1111:user/user_foo',
    ),
])

# Parse IAM entity from ARN
cloudtrail_df = cloudtrail_df.withColumn("iam", F.split(cloudtrail_df["UserIdentityArn"], ":").getItem(5))

df_udf_output = cloudtrail_df.withColumn("output", get_query_execution_udf("QueryExecutionId"))

# Order columns by the interest to users
df_final = df_udf_output.select(
    df_udf_output.QueryExecutionId.alias("query_execution_id"),
    df_udf_output.iam.alias("iam"),
    df_udf_output.output.QueryTxt.alias("query_text"),
    df_udf_output.output.State.alias("state"),
    df_udf_output.output.DataScannedInTB.alias("data_scanned_in_tb"),
    df_udf_output.output.DataScannedCostUSD.alias("data_scanned_cost_USD"),
    df_udf_output.output.SubmissionTime.alias("submission_time"),
    df_udf_output.output.CompletionDateTime.alias("completion_date_time"),
    df_udf_output.output.EngineExecutionTimeSeconds.alias("engine_execution_time_seconds"),
    df_udf_output.UserIdentityArn.alias("iam_arn"),
    df_udf_output.output.DataScannedInBytes.alias("data_scanned_in_bytes"),
    df_udf_output.output.Workgroup.alias("workgroup"),
    df_udf_output.output.Database.alias("database"),
    df_udf_output.AccountId.alias("account_id"),
    df_udf_output.output.ReusedPreviousResult.alias("reused_previous_result"),
    df_udf_output.output.SelectedEngineVersion.alias("selected_engine_version"),
    df_udf_output.output.EffectiveEngineVersion.alias("effective_engine_version"),
)

# Parse dbt model name if exists
df_final = df_final.withColumn("dbt_model", extract_dbt_model_udf(df_final["query_text"]))

df_final.show()

# Create the table if exists. Else MERGE.
# In Spark 3.3+ use spark.catalog.tableExists
if not table_exists(CATALOG_NAME, OUTPUT_DATABASE_NAME, OUTPUT_TABLE_NAME):
    df_final.writeTo(FULL_TABLE_NAME).partitionedBy(F.days("submission_time")).create()
else:
    df_final.createOrReplaceTempView("df_final")

    merge_query = f"""
    MERGE INTO {FULL_TABLE_NAME} t
    USING df_final s
    ON t.query_execution_id = s.query_execution_id
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_query)

