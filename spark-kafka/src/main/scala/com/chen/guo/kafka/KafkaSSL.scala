package com.chen.guo.kafka

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

object KafkaSSL extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaSSL")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val topicName = "AnotherTestTopic"

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", System.getenv("broker"))
    .withMTlsOptions()
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    //change to {"$topicName":{"0":1}} if you want to read from offset 1
    //.option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
    //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
    .load()

  val kafkaDF: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val query: StreamingQuery = kafkaDF.writeStream
    .queryName("kafka-ingest")
    .outputMode(OutputMode.Append())
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .format("console")
    .start()

  query.awaitTermination()
}
