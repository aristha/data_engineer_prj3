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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689603911759 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1689603911759",
)

# Script generated for node Join
Join_node1689603949987 = Join.apply(
    frame1=AWSGlueDataCatalog_node1689603911759,
    frame2=AccelerometerLanding_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689603949987",
)

# Script generated for node Drop Fields
DropFields_node1689606340193 = DropFields.apply(
    frame=Join_node1689603949987,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1689606340193",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689604062948 = DynamicFrame.fromDF(
    DropFields_node1689606340193.toDF().dropDuplicates(["user", "timestamp"]),
    glueContext,
    "DropDuplicates_node1689604062948",
)

# Script generated for node Accelerometer  Trusted
AccelerometerTrusted_node3 = glueContext.getSink(
    path="s3://tamhv2-bucket/accelerometer-trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node3",
)
AccelerometerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer-trusted"
)
AccelerometerTrusted_node3.setFormat("json")
AccelerometerTrusted_node3.writeFrame(DropDuplicates_node1689604062948)
job.commit()