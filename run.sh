#!/bin/bash
sbt --error assembly &&
~/software/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --deploy-mode client \
  --class "Main" \
  --master local[*] \
  --conf spark.driver.maxResultSize=3g \
  target/scala-2.11/Wikipedia-geoEdits-assembly-0.1.jar "$@"