import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set input and output paths
input_path = "s3://finalyelp/yelp/yelpdata.csv"
output_path = "s3://finalyelp/yelpgit/"

# Read the already transformed data (assumed CSV with header)
df = spark.read.option("header", True).csv(input_path)

# Optionally drop duplicates if needed
# df = df.dropDuplicates()

# Write the data to another location
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", True) \
  .csv(output_path)

job.commit()
