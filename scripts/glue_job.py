import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("yelp-data-processing", {})

# Read CSV from S3
df = spark.read.option("header", "true").csv("s3://finalyelp/finalyelpdata/yelp.csv")

# Write as a single CSV file
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://yelpdatasetsanthosh/yelp_data/")

job.commit()
