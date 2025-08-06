import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, year, month
from pyspark.sql.types import StringType

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load public data from S3 (replace with actual public paths if different)
b_df = spark.read.parquet("s3://public-yelp-dataset/business/")
r_df = spark.read.parquet("s3://public-yelp-dataset/review/")
u_df = spark.read.parquet("s3://public-yelp-dataset/user/")

# Rename columns
b_df = b_df.withColumnRenamed("name", "b_name")\
           .withColumnRenamed("stars", "b_stars")\
           .withColumnRenamed("review_count", "b_review_count")

r_df = r_df.withColumnRenamed("cool", "r_cool")\
           .withColumnRenamed("date", "r_date")\
           .withColumnRenamed("useful", "r_useful")\
           .withColumnRenamed("funny", "r_funny")

# Join datasets
review_user_df = r_df.join(u_df, on="user_id", how="inner")
final_df = review_user_df.join(b_df, on="business_id", how="inner")

# Select columns
columns_to_keep = [
    "business_id", "user_id", "name", "cool", "r_date", "review_id",
    "funny", "stars", "useful", "city", "review_count", "fans",
    "b_name", "state", "categories"
]
final_df = final_df.select(*columns_to_keep)

# Remove duplicates
final_df = final_df.dropDuplicates()

# Super category mapping
super_categories = {
    "Restaurants": ["Restaurants", "Food"],
    "Shopping": ["Shopping", "Fashion", "Books", "Department Stores"],
    "Beauty & Spas": ["Hair Salons", "Beauty & Spas", "Nail Salons", "Massage"],
    "Health & Medical": ["Dentists", "Health & Medical", "Chiropractors"],
    "Nightlife": ["Bars", "Nightlife", "Clubs", "Pubs"],
    "Automotive": ["Auto Repair", "Automotive", "Car Dealers"],
    "Fitness": ["Gyms", "Fitness & Instruction", "Yoga", "Trainers"],
    "Home Services": ["Home Services", "Plumbing", "Electricians"],
    "Education": ["Education", "Tutoring Centers"],
    "Pets": ["Pet Services", "Veterinarians", "Pet Stores"]
}

def map_super_category(categories):
    if categories is None:
        return "Other"
    for super_cat, keywords in super_categories.items():
        for keyword in keywords:
            if keyword in categories:
                return super_cat
    return "Other"

map_super_category_udf = udf(map_super_category, StringType())
final_df = final_df.withColumn("super_category", map_super_category_udf(final_df["categories"]))

# Extract year & month, drop unused columns
final_df = final_df.withColumn("year", year("r_date"))\
                   .withColumn("month", month("r_date"))\
                   .drop("r_date", "categories")

# Output path (your own S3)
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
output_path = f"s3://finalyelp2012310/yelpraw/{timestamp}/"


if not output_path.strip():
    raise ValueError("Output path cannot be empty.")

# Write to S3
final_df.coalesce(1) \
       .write \
       .mode("overwrite") \
       .option("header", True) \
       .csv(output_path)

job.commit()
