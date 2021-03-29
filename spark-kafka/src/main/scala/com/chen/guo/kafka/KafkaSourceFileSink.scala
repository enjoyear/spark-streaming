package com.chen.guo.kafka

import com.chen.guo.constant.Constant
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, StreamingQueryManager, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.split

import scala.concurrent.duration.DurationInt

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
object KafkaSourceFileSink {

  val appName = "KafkaIntegration"
  val queryNameKafkaIngest = "kafka-ingest"
  val queryNameRate3 = "rate-3s"
  val topicName = "quickstart-events"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName(appName)
      //.config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.stopTimeout", "5000") //in milliseconds
      .getOrCreate()

    // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
    // info log is too much
    spark.sparkContext.setLogLevel("WARN")
    // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
    spark.streams.addListener(new MyStreamingQueryListener(spark.streams))

    import spark.implicits._
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


    val qRate3s: StreamingQuery = spark.readStream
      .format("rate")
      .load
      .writeStream
      .queryName(queryNameRate3)
      .format("console")
      .trigger(Trigger.ProcessingTime(3.seconds))
      .option("truncate", false)
      .start

    //  val kafkaDF: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //    .as[(String, String)]
    val kafkaDF = df.select("value", "offset", "tokens", "col1", "col2")

    spark.streams.active.foreach(x => println(s"Before anything starts -- StreamQuery Id: ${x.id}"))

    val qKafkaIngest: StreamingQuery = kafkaDF.writeStream
      .queryName(queryNameKafkaIngest)
      .format("json")
      .option("path", Constant.OutputPath)
      .option("checkpointLocation", Constant.CheckpointLocation)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    spark.streams.active.foreach(x => println(s"After something started -- Active query: ${x.name}"))

    sys.addShutdownHook(() => {
      println("Gracefully stopping Spark Streaming Application")
      qKafkaIngest.stop
    })

    terminateOption1(spark)
    //terminateOption2(spark)

    println(s"Exiting structured streaming application ${appName} now...")
  }

  def terminateOption1(spark: SparkSession): Unit = {
    spark.streams.active.foreach(x => x.awaitTermination())
  }

  /**
    * References
    * https://stackoverflow.com/questions/52762405/how-to-start-multiple-streaming-queries-in-a-single-spark-application?rq=1
    * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-demo-StreamingQueryManager-awaitAnyTermination-resetTerminated.html
    */
  def terminateOption2(spark: SparkSession): Unit = {
    while (!spark.streams.active.isEmpty) {
      println("Queries currently still active: " + spark.streams.active.map(x => x.name).mkString(","))
      spark.streams.awaitAnyTermination()
      spark.streams.resetTerminated()
    }
  }
}

class MyStreamingQueryListener(sqm: StreamingQueryManager) extends StreamingQueryListener {
  var maxUpdates = 5

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    /**
      * A unique query id that persists across restarts. {@link StreamingQuery.id}
      * RunId is a query id that is unique for every start/restart. {@link StreamingQuery.runId}
      * User specified {@link StreamingQuery.name} of the query. If set, must be unique across all active queries.
      */
    println(s"Query started at ${queryStarted.timestamp}: name ${queryStarted.name}, query id ${queryStarted.id}, run id ${queryStarted.runId}")
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    println(s"Query terminated: query id ${queryTerminated.id}, run id ${queryTerminated.runId}, " +
      s"exception ${queryTerminated.exception.getOrElse("n/a")}")
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    //println("Query made progress: " + queryProgress.progress)
    println(s"Query ${queryProgress.progress.name} made progress")

    /**
      * TODO: how to terminate a structured streaming application gracefully?
      * 1. https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-demo-StreamingQueryManager-awaitAnyTermination-resetTerminated.html
      * 2. https://stackoverflow.com/questions/45717433/stop-structured-streaming-query-gracefully
      * 3. Control by an external file?
      * https://medium.com/@manojkumardhakad/how-to-do-graceful-shutdown-of-spark-streaming-job-9c910770349c
      */
    maxUpdates = maxUpdates - 1
    if (maxUpdates <= 0) {
      sqm.active.foreach(query => {
        if (query.name == KafkaSourceFileSink.queryNameRate3 && maxUpdates > -5) {
          return
        }

        /**
          * query.status example
          * {
          * "message" : "Waiting for data to arrive",
          * "isDataAvailable" : false,
          * "isTriggerActive" : false
          * }
          */
        println(s"Status for ${query.name}: ${query.status}")
        println(s"Terminating query ${query.name} ${query.id} ${query.runId}")
        //print exception if any
        query.exception.foreach(streamingQueryException => println(streamingQueryException.toString()))

        if (!query.status.isDataAvailable
          && !query.status.isTriggerActive
          && !query.status.message.equals("Initializing sources")) {
          query.stop()
        }
        query.stop()
      })
    }

    def canStop(query: StreamingQuery, awaitTerminationTimeMs: Long) {
      while (query.isActive) {
        val msg = query.status.message
        if (!query.status.isDataAvailable
          && !query.status.isTriggerActive
          && !msg.equals("Initializing sources")) {
          query.stop()
        }
        query.awaitTermination(awaitTerminationTimeMs)
      }
    }
  }

}