package com.chen.guo.kafka

import org.apache.spark.sql.{Dataset, SparkSession}

//TODO, add consumer id and consumer group
object KafkaSourceBatch extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val topicName = "quickstart-events"

  val topicAll = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()

  val topicAllDF: Dataset[(String, String)] = topicAll.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  topicAllDF.foreach(println(_))

  println("topicAll done: it prints out the entire content of a topic\n")


  val topicRange = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    .option("startingOffsets", s"""{"$topicName":{"0":1}}""")
    .option("endingOffsets", s"""{"$topicName":{"0":3}}""")
    .load()

  val topicRangeDF: Dataset[(String, String)] = topicRange.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  topicRangeDF.foreach(println(_))
  println("topicRange done: it prints out 2 messages, topicPartition at offset 1 & 2\n")
}
