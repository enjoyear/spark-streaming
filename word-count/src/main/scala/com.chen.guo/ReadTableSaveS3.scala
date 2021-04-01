package com.chen.guo

import org.apache.spark.sql._

object ReadTableSaveS3 extends App {
  val spark = SparkSession
    .builder
    .appName("WordCount")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val outputPath = "/tmp/chenguo/test2"

  val df = spark.table("chen_guo.diamonds")
  df.write.mode("overwrite").json(outputPath)
}

