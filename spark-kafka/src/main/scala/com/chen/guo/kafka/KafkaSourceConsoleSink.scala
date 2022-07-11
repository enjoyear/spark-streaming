package com.chen.guo.kafka

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat

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
  val logger = LoggerFactory.getLogger(this.getClass)
  val spark = SparkSession
    .builder
    .master("local[1]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val topicName = "TestTopic"

  val df = spark
    .readStream
    .format("kafka")
    //.option("kafka.bootstrap.servers", "ec2-54-162-247-190.compute-1.amazonaws.com:4000")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    //change to {"$topicName":{"0":1}} if you want to read from offset 1
    //.option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
    //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
    .load()

  val kafkaDF: Dataset[(String, String)] = df
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as val")
    .as[(String, String)]

  // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
  spark.streams.addListener(new PausableStreamQueryListener(spark.streams))

  val udfWithException = new UDFWithException()
  spark.udf.register("throwException", (x: Int) => udfWithException.throwException(x))

  val query: StreamingQuery = kafkaDF
    .selectExpr("key", "throwException(CAST(val AS INT))")
    .writeStream
    .queryName("kafka-ingest")
    .outputMode(OutputMode.Append())
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .format("console")
    .start()

  logger.info("Stream started.")
  try {
    // This is a blocking call
    query.awaitTermination()
  } catch {
    case t: Throwable =>
      logger.info(s"Stream terminated. Got exception: $t")
  }
}

class UDFWithException {
  def throwException(num: Int): Int = {
    if (num == 1) {
      return num
    }
    throw new RuntimeException(s"Got unexpected number $num")
  }
}

class PausableStreamQueryListener(sqm: StreamingQueryManager) extends StreamingQueryListener {
  val logger = LoggerFactory.getLogger(this.getClass)
  val id2Name = scala.collection.mutable.HashMap[java.util.UUID, String]()

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logger.info(s"Query ${queryStarted.name} started at ${queryStarted.timestamp}: id ${queryStarted.id}, run id ${queryStarted.runId}")
    id2Name(queryStarted.id) = queryStarted.name
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    val queryName = id2Name(queryTerminated.id)

    logger.info(s"Query terminated: id ${queryTerminated.id}, run id ${queryTerminated.runId}, " +
      s"exception ${queryTerminated.exception.getOrElse("n/a")}")
  }

  // for monitoring purpose
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    val queryName = id2Name(queryProgress.progress.id)

    logger.info(s"${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(System.currentTimeMillis)}: Query ${queryProgress.progress.name} made progress. " +
      s"Time take for retrieving latestOffset: ${queryProgress.progress.durationMs.get("latestOffset")}")

    if (queryProgress.progress.numInputRows <= 0)
      return

    queryProgress.progress.sources.foreach(source => {
      logger.info(s"InputRows/s ${source.inputRowsPerSecond}, ProcessedRows/s ${source.processedRowsPerSecond}")
    })
  }

}