from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

spark = (SparkSession.builder
         .appName("spark_stream")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

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

# readStream = keep reading, only new events (startingOffsets = latest)
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "broker:19092")
       .option("subscribe", "wiki_changes")
       .option("startingOffsets", "latest")
       .load())

events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.col("event_time").cast("timestamp")))

# Count per one-minute window and type, waiting up to 2 minutes for late events
counts = (events
          .withWatermark("event_time", "2 minutes")
          .groupBy(F.window("event_time", "1 minute").alias("w"), "type")
          .count()
          .select(F.col("w.start").alias("minute_utc"), "type", "count"))

query = (counts.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", "false")
         .option("numRows", 50)
         .trigger(processingTime="10 seconds")
         .start())

print("Streaming started. Start producer.py in a second terminal now.")
query.awaitTermination(180)   # stop by itself after 3 minutes
query.stop()
print("Streaming stopped.")
