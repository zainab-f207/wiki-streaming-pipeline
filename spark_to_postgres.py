import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

spark = (SparkSession.builder
         .appName("spark_to_postgres")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

PASSWORD = os.environ["POSTGRES_PASSWORD"]

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

# earliest = on the very first run, start from the oldest stored message.
# After that, the checkpoint decides where to continue.
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "broker:19092")
       .option("subscribe", "wiki_changes")
       .option("startingOffsets", "earliest")
       .load())

events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.col("event_time").cast("timestamp")))

counts = (events
          .withWatermark("event_time", "2 minutes")
          .groupBy(F.window("event_time", "1 minute").alias("w"), "type")
          .count()
          .select(F.col("w.start").alias("minute_utc"), "type", F.col("count").alias("events")))


def save_batch(batch_df, batch_id):
    (batch_df.write.format("jdbc")
     .option("url", "jdbc:postgresql://postgres:5432/wiki")
     .option("dbtable", "wiki_minute_counts")
     .option("user", "postgres")
     .option("password", PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .save())
    print("Saved batch", batch_id)


query = (counts.writeStream
         .outputMode("append")
         .foreachBatch(save_batch)
         .option("checkpointLocation", "/checkpoints/wiki_minute_counts")
         .trigger(processingTime="10 seconds")
         .start())

print("Streaming started. Start producer.py in a second terminal now.")
query.awaitTermination(300)   # stop by itself after 5 minutes
query.stop()
print("Streaming stopped.")
