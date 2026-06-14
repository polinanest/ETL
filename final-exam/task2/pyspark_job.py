from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, round

spark = SparkSession.builder.appName("etl-applications").getOrCreate()

S3_BUCKET = "s3a://etl002"
INPUT_PATH = f"{S3_BUCKET}/input/applications.csv"
OUTPUT_PATH = f"{S3_BUCKET}/output/applications_agg"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)

agg = df.groupBy("region_code", "product_type", "decision_status").agg(
    count("application_id").alias("total_applications"),
    round(avg("credit_score"), 2).alias("avg_credit_score"),
    round(avg("requested_amount"), 2).alias("avg_requested_amount"),
    round(sum("approved_amount"), 2).alias("total_approved_amount")
)

agg.write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)

print(f"Done. Total rows processed: {df.count()}")
spark.stop()
