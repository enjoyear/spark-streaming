package com.chen.guo

import com.chen.guo.constant.Constant
import com.chen.guo.kafka.KafkaSourceFileSink.appName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWC extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName(appName)
    .getOrCreate()

  val text_file: RDD[String] = spark.sparkContext.textFile(Constant.TextSampleFiles)

  val counts: RDD[(String, Int)] = text_file.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((a, b) => a + b)

  counts.foreach(println)

}
