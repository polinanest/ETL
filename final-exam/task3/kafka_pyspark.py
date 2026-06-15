from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

spark = SparkSession.builder.appName("kafka-loan-reader").getOrCreate()

KAFKA_BROKER = "rc1a-3k1ta5efkqovuo83.mdb.yandexcloud.net:9091"
TOPIC = "loan-applications"
KAFKA_USER = "kafka-user"
KAFKA_PASSWORD = "admin123"  
S3_OUTPUT = "s3a://etl002/output/kafka_loans"

schema = StructType([
    StructField("application_id", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType()),
        StructField("term_months", IntegerType())
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType()),
        StructField("risk_level", StringType())
    ])),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("status", StringType())
    ]))),
    StructField("decision_status", StringType()),
    StructField("submitted_at", StringType())
])

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";') \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

flat = parsed.select(
    col("application_id"),
    col("customer.customer_id"),
    col("customer.region"),
    col("loan.amount").alias("loan_amount"),
    col("loan.term_months"),
    col("scoring.score").alias("credit_score"),
    col("scoring.risk_level"),
    col("decision_status"),
    col("submitted_at"),
    explode(col("documents")).alias("doc")
).select(
    col("application_id"),
    col("customer_id"),
    col("region"),
    col("loan_amount"),
    col("term_months"),
    col("credit_score"),
    col("risk_level"),
    col("decision_status"),
    col("submitted_at"),
    col("doc.type").alias("doc_type"),
    col("doc.status").alias("doc_status")
)

flat.write.mode("overwrite").option("header", "true").csv(S3_OUTPUT)

print(f"Done. Rows written: {flat.count()}")
spark.stop()
