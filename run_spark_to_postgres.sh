set -a
source .env
set +a
docker rm -f spark_pg 2>/dev/null
MSYS_NO_PATHCONV=1 docker create --name spark_pg -u root --memory=3g --network wiki-streaming-pipeline_default -e POSTGRES_PASSWORD -v spark_ivy:/tmp/ivy -v spark_checkpoints:/checkpoints apache/spark:3.5.9-python3 /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.10 /tmp/spark_to_postgres.py
MSYS_NO_PATHCONV=1 docker cp spark_to_postgres.py spark_pg:/tmp/spark_to_postgres.py
docker start -a spark_pg
docker rm spark_pg
