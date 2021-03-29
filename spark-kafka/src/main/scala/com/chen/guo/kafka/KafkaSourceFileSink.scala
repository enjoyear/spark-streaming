package com.chen.guo.kafka

import com.chen.guo.constant.Constant
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Read {@link KafkaSourceConsoleSink}'s doc for more setup details
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-MicroBatchExecution.html
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-offsets-and-metadata-checkpointing.html
  *
  * ├── checkpoint
  * │   ├── commits
  * │   │   ├── 0
  * │   │   ├── 1
  * │   │   └── 2
  * │   ├── metadata  //keeps the id for the writeStream, but not run id
  * │   ├── offsets   //WAL of Offsets
  * │   │   ├── 0
  * │   │   ├── 1
  * │   │   └── 2
  * │   └── sources
  * │       └── 0
  * │           └── 0
  * ├── ingested
  * │   ├── _spark_metadata
  * │   │   ├── 0
  * │   │   ├── 1
  * │   │   └── 2
  * │   ├── part-00000-304e6ea5-368a-485b-ad99-12594bb58de8-c000.json
  * │   ├── part-00000-84d67966-b3d7-4e95-8a32-514008a43fc3-c000.json
  * │   └── part-00000-adc7631d-98fc-493e-be05-4ea1aa1d5f57-c000.json
  */
object KafkaSourceFileSink extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import org.apache.spark.sql.functions.split
  import spark.implicits._

  val topicName = "quickstart-events"

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    .option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
    //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
    .load()
    //https://stackoverflow.com/questions/46130191/why-do-id-and-runid-change-every-start-of-a-streaming-query
    .withColumn("tokens", split('value, " "))
    .withColumn("col1", 'tokens(0) cast "string")
    .withColumn("col2", 'tokens(1) cast "string")

  val kafkaDF: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val query: StreamingQuery = kafkaDF.writeStream
    .queryName("kafka-ingest")

    .format("json")
    .option("path", Constant.OutputPath)
    .option("checkpointLocation", Constant.CheckpointLocation)
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .start()
  query.id
  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      /**
        * A unique query id that persists across restarts. {@link StreamingQuery.id}
        */
      println("Query started: " + queryStarted.id)

      /**
        * A query id that is unique for every start/restart. {@link StreamingQuery.runId}
        */
      println("Query started: " + queryStarted.runId)

      /**
        * User specified name of the query. {@link StreamingQuery.name}
        * This name, if set, must be unique across all active queries.
        */
      println("Query started: " + queryStarted.name)
    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      //println("Query made progress: " + queryProgress.progress)
      println("Query made progress")
    }
  })

  println(s"Awaiting query ${query.name} ${query.id} with run ${query.runId} to terminate")
  // Start multiple streaming queries in a single spark application
  // https://stackoverflow.com/questions/52762405/how-to-start-multiple-streaming-queries-in-a-single-spark-application?rq=1
  spark.streams.awaitAnyTermination()
}
