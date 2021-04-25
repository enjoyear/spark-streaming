package com.chen.guo.kafka

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  *
  * Reads data from Kafka. It’s compatible with Kafka broker versions 0.10.0 or higher.
  *
  *
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-kafka-data-source.html
  * KafkaSourceProvider is a DataSourceRegister
  *
  * Useful Examples:
  * https://github.com/supergloo/spark-streaming-examples/blob/master/kafka/src/main/scala/com/supergloo/kafka_examples/SimpleKafka.scala
  *
  * How to run:
  * Setup local Kafka cluster as in https://kafka.apache.org/quickstart
  */
object KafkaSourceConsoleSink extends App {

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

  val df = spark
    .readStream
    .format("kafka")
    //.option("kafka.bootstrap.servers", "ec2-54-162-247-190.compute-1.amazonaws.com:4000")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    //change to {"$topicName":{"0":1}} if you want to read from offset 1
    .option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
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
