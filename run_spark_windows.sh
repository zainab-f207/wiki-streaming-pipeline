docker rm -f spark_job 2>/dev/null
MSYS_NO_PATHCONV=1 docker create --name spark_job -u root --memory=3g --network wiki-streaming-pipeline_default apache/spark:3.5.9-python3 /opt/spark/bin/spark-submit --master "local[1]" --driver-memory 2g --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /tmp/spark_windows.py
MSYS_NO_PATHCONV=1 docker cp spark_windows.py spark_job:/tmp/spark_windows.py
docker start -a spark_job
docker rm spark_job
