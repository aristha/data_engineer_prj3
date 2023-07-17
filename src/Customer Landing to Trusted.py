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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689598447504 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1689598447504",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource where sharewithresearchasofdate is not null
"""
SQLQuery_node1689603611900 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AWSGlueDataCatalog_node1689598447504},
    transformation_ctx="SQLQuery_node1689603611900",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689603649229 = DynamicFrame.fromDF(
    SQLQuery_node1689603611900.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1689603649229",
)

# Script generated for node Amazon S3
AmazonS3_node1689603508669 = glueContext.getSink(
    path="s3://tamhv2-bucket/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1689603508669",
)
AmazonS3_node1689603508669.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1689603508669.setFormat("json")
AmazonS3_node1689603508669.writeFrame(DropDuplicates_node1689603649229)
job.commit()