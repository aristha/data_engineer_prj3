import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customers trusted
customerstrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customerstrusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1689004974152 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1689004974152",
)

# Script generated for node SQL Query
SqlQuery151 = """
select distinct customer_trusted.* from customer_trusted inner join accelerometer_trusted on customer_trusted.email=accelerometer_trusted.user
"""
SQLQuery_node1689006604305 = sparkSqlQuery(
    glueContext,
    query=SqlQuery151,
    mapping={
        "customer_trusted": customerstrusted_node1,
        "accelerometer_trusted": accelerometer_trusted_node1689004974152,
    },
    transformation_ctx="SQLQuery_node1689006604305",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689005080351 = DynamicFrame.fromDF(
    SQLQuery_node1689006604305.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1689005080351",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1689005080351,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://tam-p3/customer/curated/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
