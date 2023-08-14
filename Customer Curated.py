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

# Script generated for node Customer Trusted
CustomerTrusted_node1691776589058 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1691776589058",
)

# Script generated for node Accelerometr LAnding
AccelerometrLAnding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometrLAnding_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1691775536945 = Join.apply(
    frame1=AccelerometrLAnding_node1,
    frame2=CustomerTrusted_node1691776589058,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1691775536945",
)

# Script generated for node Drop Fields
DropFields_node1691776183599 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1691775536945,
    paths=["user", "x", "y", "z"],
    transformation_ctx="DropFields_node1691776183599",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691776183599,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://awsbucketg/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
