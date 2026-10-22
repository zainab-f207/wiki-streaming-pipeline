docker rm -f spark_stream 2>/dev/null
MSYS_NO_PATHCONV=1 docker create --name spark_stream -u root --memory=3g --network wiki-streaming-pipeline_default -v spark_ivy:/tmp/ivy apache/spark:3.5.9-python3 /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /tmp/spark_stream.py
MSYS_NO_PATHCONV=1 docker cp spark_stream.py spark_stream:/tmp/spark_stream.py
docker start -a spark_stream
docker rm spark_stream
