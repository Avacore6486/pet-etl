#!/bin/bash
docker exec spark_app_pet /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-apps/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark-apps/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/spark-apps/kafka-clients-3.4.1.jar,/opt/spark-apps/hadoop-aws-3.3.4.jar,/opt/spark-apps/aws-java-sdk-bundle-1.12.262.jar,/opt/spark-apps/commons-pool2-2.11.1.jar \
  /opt/spark-apps/kafka_to_s3.py
