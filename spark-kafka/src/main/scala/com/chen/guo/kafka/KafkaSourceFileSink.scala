package com.chen.guo.kafka

import com.chen.guo.constant.Constant
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Read {@link KafkaSourceConsoleSink}'s doc for more setup details
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-MicroBatchExecution.html
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-offsets-and-metadata-checkpointing.html
  *
  * ├── checkpoint
  * │   ├── commits
  * │   │   ├── 0     //3 for each batch: step 4. add a file indicating committed
  * │   │   ├── 1
  * │   │   └── 2
  * │   ├── metadata        //1. mark down the writeStream id(e.g. {"id":"6620ab39-04f9-4af4-8e50-9e5dedff1205"}) as the first thing to do
  * │   ├── offsets
  * │   │   ├── 0     //3 for each batch: step 1. mark down the max offset for a topic partition and the configurations for current batch job
  * │   │   ├── 1
  * │   │   └── 2
  * │   └── sources
  * │       └── 0
  * │           └── 0       //2. before any batch starts, mark down the beginning offset for topic partitions. e.g. {"quickstart-events":{"0":11}}
  * ├── ingested
  * │   ├── _spark_metadata
  * │   │   ├── 0     //3 for each batch: step 3. keep the output metadata(e.g. filename, size, etc.) in a file
  * │   │   ├── 1
  * │   │   └── 2
  * │   ├── part-00000-304e6ea5-368a-485b-ad99-12594bb58de8-c000.json    //3 for each batch: step 2. save the batch output to disk
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

  val df: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicName)
    //start consuming from the earliest. By default it will be the latest, which is to discard all history
    //.option("startingOffsets", "earliest")
    .option("startingOffsets", s"""{"$topicName":{"0":-2}}""")
    //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
    .load()

    /**
      * The default loaded data frame. Below is a row as an example.
      * {
      * "value":"dGhpcyBpcyBtNg==",                    //base64 encoded bytes
      * "topic":"quickstart-events",
      * "partition":0,
      * "offset":5,
      * "timestamp":"2021-03-28T18:22:18.819-07:00",   //message produced time
      * "timestampType":0
      * }
      */
    // add extra columns
    // https://stackoverflow.com/questions/46130191/why-do-id-and-runid-change-every-start-of-a-streaming-query
    .withColumn("tokens", split('value, " "))
    .withColumn("col1", 'tokens(0) cast "string")
    .withColumn("col2", 'tokens(1) cast "string")


  //  val kafkaDF: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  //    .as[(String, String)]
  val kafkaDF = df.select("value", "offset", "tokens", "col1", "col2")

  spark.streams.active.foreach(x => println(s"Before anything starts -- StreamQuery Id: ${x.id}"))

  // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
  spark.streams.addListener(new MyStreamingQueryListener())

  val query: StreamingQuery = kafkaDF.writeStream
    .queryName("kafka-ingest")
    .format("json")
    .option("path", Constant.OutputPath)
    .option("checkpointLocation", Constant.CheckpointLocation)
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime("3 seconds"))
    .start()

  spark.streams.active.foreach(x => println(s"After something started -- StreamQuery Id: ${x.id}"))

  println(s"Awaiting query ${query.name} ${query.id} with run ${query.runId} to terminate")
  // Start multiple streaming queries in a single spark application
  // https://stackoverflow.com/questions/52762405/how-to-start-multiple-streaming-queries-in-a-single-spark-application?rq=1
  spark.streams.awaitAnyTermination()
}

class MyStreamingQueryListener extends StreamingQueryListener {
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
}