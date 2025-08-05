from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("final", {})  # 'final' is the job name

# Read cleaned CSV from S3
input_path = "s3://finalyelp/finalyelpdata/yelp.csv"
df = spark.read.option("header", "true").csv(input_path)

# Coalesce to single file and write to output
output_path = "s3://yelpdatasetsanthosh/yelp_data/"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

job.commit()
