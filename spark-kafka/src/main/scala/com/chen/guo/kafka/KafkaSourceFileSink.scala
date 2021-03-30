package com.chen.guo.kafka

import com.chen.guo.constant.Constant
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
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
  *
  *
  * Create topics with keys. Follow examples here
  * https://stackoverflow.com/questions/62070151/how-to-send-key-value-messages-with-the-kafka-console-producer
  *
  * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example-topic \
  * --property "parse.key=true" --property "key.separator=:"
  *
  * emp100:{"emp_id":100,"first_name":"Keshav","last_name":"Lodhi"}
  * emp101:{"emp_id":101,"first_name":"Mike","last_name":"M"}
  *
  * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic example-topic
  */
object KafkaSourceFileSink {

  val appName = "KafkaIntegration"
  val queryNameKafkaIngest = "kafka-ingest"
  val queryNameRate3 = "rate-3s"
  val topicName = "quickstart-events"
  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName(appName)
      .getOrCreate()

    // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
    // info log is too much
    spark.sparkContext.setLogLevel("WARN")
    // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
    spark.streams.addListener(new MyStreamingQueryListener(spark.streams))

    if (false) {
      //Flip to add one more streaming query to this example
      val qRate3s: StreamingQuery = spark.readStream
        .format("rate")
        .load
        .writeStream
        .queryName(queryNameRate3)
        .format("json")
        .option("path", getLocalPath(Constant.OutputPath, queryNameRate3))
        .option("checkpointLocation", getLocalPath(Constant.CheckpointLocation, queryNameRate3))
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime(3.seconds))
        .start()
    }

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
        * The original data frame loaded. See the row below as an example.
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

    val kafkaIngestWriter: DataStreamWriter[Row] = kafkaDF.writeStream
      .queryName(queryNameKafkaIngest)
      .format("json")
      .option("path", getLocalPath(Constant.OutputPath, queryNameKafkaIngest))
      .option("checkpointLocation", getLocalPath(Constant.CheckpointLocation, queryNameKafkaIngest))
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("3 seconds"))

    //"kafka-ingest" won't be active unless started
    println(s"Active queries: ${spark.streams.active.map(x => s"${x.name}(${x.id})").mkString(",")}")
    val qKafkaIngest: StreamingQuery = kafkaIngestWriter.start()
    //"kafka-ingest" will be included
    println(s"Active queries: ${spark.streams.active.map(x => s"${x.name}(${x.id})").mkString(",")}")

    //stopQueriesOption1(spark)
    //stopQueriesOption2(spark)
    addShutdownHook(spark)

    waitAllTerminationOption1(spark)
    //checkTerminationOption2(spark)

    executor.shutdown()
    println(s"Exiting structured streaming application ${appName} now...")
  }

  def getLocalPath(rootDir: String, queryName: String) = s"${rootDir}/${queryName}"

  /**
    * Stop queries based on a fixed schedule
    */
  def stopQueriesOption1(spark: SparkSession): Unit = {
    val map = spark.streams.active.map(x => (x.name, x)).toMap
    executor.schedule(new Runnable {
      override def run(): Unit = map(queryNameKafkaIngest).stop()
    }, 10, TimeUnit.SECONDS)
    executor.schedule(new Runnable {
      override def run(): Unit = map(queryNameRate3).stop()
    }, 20, TimeUnit.SECONDS)
  }

  /**
    * Stop queries based on some conditions
    */
  def stopQueriesOption2(spark: SparkSession): Unit = {
    executor.schedule(new Runnable {
      override def run(): Unit = spark.streams.active.foreach(query => {
        if (canStop(query)) {
          query.stop()
        }
      })
    }, 30, TimeUnit.SECONDS)
  }

  /**
    * Alternative logic can be checking a status file on disk/database at a fixed schedule
    */
  def canStop(query: StreamingQuery): Boolean = {
    /**
      * query.status example
      * {
      * "message" : "Waiting for data to arrive",
      * "isDataAvailable" : false,
      * "isTriggerActive" : false
      * }
      */
    println(s"Status for ${query.name}: ${query.status}")
    //print exception if any
    query.exception.foreach(streamingQueryException => println(streamingQueryException.toString()))

    // rate-3s won't stop because isDataAvailable is true
    !query.status.isDataAvailable && !query.status.isTriggerActive && !query.status.message.equals("Initializing sources")
  }

  /**
    * To stop
    * ps -ef | grep KafkaSourceFileSink | grep -v grep | awk '{print $2}' | xargs kill -s SIGTERM
    */
  def addShutdownHook(spark: SparkSession): Unit = {
    /**
      * Be careful, don't use ()=>{}
      * https://stackoverflow.com/questions/26944515/scala-shutdown-hooks-never-running
      * https://stackoverflow.com/questions/4543228/whats-the-difference-between-and-unit
      */
    sys.addShutdownHook({
      // TODO: What are the impacts if kill directly without stopping the queries?
      // For example, the application or cluster is terminated directly
      println("Got kill signal. Stopping the Spark Context directly without stopping the queries.")
      //      spark.streams.active.foreach(x => {
      //        println(s"Stopping the query ${x.name} ${x.id}")
      //        x.stop() // will fail because "Cannot call methods on a stopped SparkContext."
      //      })
      println(s"Currently active queries count: ${spark.streams.active.length}")
    })
  }

  /**
    * Await all active queries to terminate
    */
  def waitAllTerminationOption1(spark: SparkSession): Unit = {
    spark.streams.active.foreach(x => x.awaitTermination())
  }

  /**
    * Await all active queries to terminate
    * References
    * https://stackoverflow.com/questions/52762405/how-to-start-multiple-streaming-queries-in-a-single-spark-application?rq=1
    * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-demo-StreamingQueryManager-awaitAnyTermination-resetTerminated.html
    */
  def waitAllTerminationOption2(spark: SparkSession): Unit = {
    while (!spark.streams.active.isEmpty) {
      println("Queries currently still active: " + spark.streams.active.map(x => x.name).mkString(","))
      //await any to terminate
      spark.streams.awaitAnyTermination()
      spark.streams.resetTerminated()
    }
  }
}

class MyStreamingQueryListener(sqm: StreamingQueryManager) extends StreamingQueryListener {
  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    /**
      * A unique query id that persists across restarts. {@link StreamingQuery.id}
      * RunId is a query id that is unique for every start/restart. {@link StreamingQuery.runId}
      * User specified {@link StreamingQuery.name} of the query. If set, must be unique across all active queries.
      */
    println(s"Query ${queryStarted.name} started at ${queryStarted.timestamp}: id ${queryStarted.id}, run id ${queryStarted.runId}")
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    println(s"Query terminated: id ${queryTerminated.id}, run id ${queryTerminated.runId}, " +
      s"exception ${queryTerminated.exception.getOrElse("n/a")}")
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    //println("Query made progress: " + queryProgress.progress)
    println(s"Query ${queryProgress.progress.name} made progress")
  }
}