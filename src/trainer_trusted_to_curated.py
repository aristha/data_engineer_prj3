import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1689607406272 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1689607406272",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1689607408338 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer-trusted",
    transformation_ctx="accelerometer_trusted_node1689607408338",
)

# Script generated for node Join
Join_node1689607455666 = Join.apply(
    frame1=accelerometer_trusted_node1689607408338,
    frame2=step_trainer_trusted_node1689607406272,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1689607455666",
)

# Script generated for node Amazon S3
AmazonS3_node1689607711820 = glueContext.getSink(
    path="s3://tamhv2-bucket/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1689607711820",
)
AmazonS3_node1689607711820.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1689607711820.setFormat("json")
AmazonS3_node1689607711820.writeFrame(Join_node1689607455666)
job.commit()