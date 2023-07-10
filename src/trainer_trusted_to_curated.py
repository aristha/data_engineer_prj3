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

# Script generated for node Customer Curated
CustomerCurated_node1689005884552 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="CustomerCurated_node1689005884552",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1689010057760 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tam-p3/step_trainer/"], "recurse": True},
    transformation_ctx="step_trainer_trusted_node1689010057760",
)

# Script generated for node SQL Query
SqlQuery129 = """
select step_trainer_trusted.* from step_trainer_trusted inner join customer on customer.serialNumber = step_trainer_trusted.serialNumber
"""
SQLQuery_node1689009307731 = sparkSqlQuery(
    glueContext,
    query=SqlQuery129,
    mapping={
        "customer": CustomerCurated_node1689005884552,
        "step_trainer_trusted": step_trainer_trusted_node1689010057760,
    },
    transformation_ctx="SQLQuery_node1689009307731",
)

# Script generated for node Drop Fields
DropFields_node1689008030709 = DropFields.apply(
    frame=SQLQuery_node1689009307731,
    paths=[],
    transformation_ctx="DropFields_node1689008030709",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689007600591 = DynamicFrame.fromDF(
    DropFields_node1689008030709.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1689007600591",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://tam-p3/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropDuplicates_node1689007600591)
job.commit()
