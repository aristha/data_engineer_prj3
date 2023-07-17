import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelermimeter trusted
accelermimetertrusted_node1689604562589 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer-trusted",
    transformation_ctx="accelermimetertrusted_node1689604562589",
)

# Script generated for node customers trusted
customerstrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customerstrusted_node1",
)

# Script generated for node Join
Join_node1689604590101 = Join.apply(
    frame1=accelermimetertrusted_node1689604562589,
    frame2=customerstrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1689604590101",
)

# Script generated for node Drop Fields
DropFields_node1689604940008 = DropFields.apply(
    frame=Join_node1689604590101,
    paths=["timestamp", "user", "x", "y", "z"],
    transformation_ctx="DropFields_node1689604940008",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689604607632 = DynamicFrame.fromDF(
    DropFields_node1689604940008.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1689604607632",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://tamhv2-bucket/customers_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1689604607632)
job.commit()