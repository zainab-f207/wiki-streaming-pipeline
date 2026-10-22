from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

spark = (SparkSession.builder
         .appName("spark_windows")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# What one JSON message contains
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("wiki", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("is_bot", BooleanType()),
    StructField("byte_change", LongType()),
])

# Read everything stored in the topic
raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", "broker:19092")
       .option("subscribe", "wiki_changes")
       .option("startingOffsets", "earliest")
       .load())

print("Messages in Kafka:", raw.count())

# Turn the raw bytes into proper columns
events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.col("event_time").cast("timestamp")))

print("Rows that failed to parse:", events.filter(F.col("event_id").isNull()).count())

# Count events per one-minute window, one column per type
table = (events
         .groupBy(F.window("event_time", "1 minute").alias("w"))
         .pivot("type", ["edit", "new", "log", "categorize"])
         .count()
         .na.fill(0)
         .select(F.col("w.start").alias("minute_utc"), "edit", "new", "log", "categorize")
         .withColumn("total", F.col("edit") + F.col("new") + F.col("log") + F.col("categorize"))
         .orderBy("minute_utc"))

table.show(30, False)
print("Sum of all windows:", table.agg(F.sum("total")).collect()[0][0])
