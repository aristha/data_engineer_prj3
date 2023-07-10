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

# Script generated for node Customer landing
Customerlanding_node1688998940009 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="Customerlanding_node1688998940009",
)

# Script generated for node SQL Query
SqlQuery107 = """
select * from myDataSource where sharewithresearchasofdate is not null
"""
SQLQuery_node1689003774473 = sparkSqlQuery(
    glueContext,
    query=SqlQuery107,
    mapping={"myDataSource": Customerlanding_node1688998940009},
    transformation_ctx="SQLQuery_node1689003774473",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1689003523702 = DynamicFrame.fromDF(
    SQLQuery_node1689003774473.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1689003523702",
)

# Script generated for node Amazon S3
AmazonS3_node1689001576401 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1689003523702,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://tam-p3/customer/trusted/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1689001576401",
)

job.commit()
