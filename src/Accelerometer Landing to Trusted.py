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
AccelerometerLanding_node1688998799810 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1688998799810",
)

# Script generated for node Customer Landing
CustomerLanding_node1688998832598 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerLanding_node1688998832598",
)

# Script generated for node Join
Join_node1689004209447 = Join.apply(
    frame1=CustomerLanding_node1688998832598,
    frame2=AccelerometerLanding_node1688998799810,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689004209447",
)

# Script generated for node Drop Fields
DropFields_node1688998512008 = DropFields.apply(
    frame=Join_node1689004209447,
    paths=[
        "customername",
        "email",
        "birthday",
        "phone",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1688998512008",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689004246751 = DynamicFrame.fromDF(
    DropFields_node1688998512008.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1689004246751",
)

# Script generated for node Amazon S3
AmazonS3_node1689001707901 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1689004246751,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tam-p3/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1689001707901",
)

job.commit()
