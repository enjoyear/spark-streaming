package com.chen.guo.kafka.dev

import com.chen.guo.constant.Constant
import com.chen.guo.kafka.KafkaSourceConsoleSink
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.DurationInt

/**
  *
  * Read {@link KafkaSourceConsoleSink}'s doc for more setup details
  *
  * Step 1: Create the testing topic
  * bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic example-topic
  * bin/kafka-topics.sh --create --topic example-topic --bootstrap-server localhost:9092 \
  * --partitions 3 --replication-factor 1
  * bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic example-topic
  *
  * Step 2: Start the consumer
  * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic example-topic
  *
  * Step 3: Start the producer
  * bin/kafka-console-producer.sh --help
  * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example-topic \
  * --property "parse.key=true" --property "key.separator=:"
  *
  * Step 4: Populate some events
  * emp100:{"emp_id":100,"first_name":"Keshav","last_name":"L"}
  * emp101:{"emp_id":101,"first_name":"Mike","last_name":"M"}
  * emp105:{"emp_id":105,"first_name":"Tree","last_name":"T"}
  *
  * Bad messages
  * emp102:{"emp_id":"102c","first_name":"BadIdType","last_name":"B"}
  * emp103:{"emp_id":103,"first_name":"ExtraField","last_name":"E", "age":33}
  * emp104:{"first_name":"MissingField","last_name":"M"}
  *
  * Other commands:
  * List kafka consumer groups(Ref: https://www.baeldung.com/ops/listing-kafka-consumers)
  * -- ./bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
  * -- ./bin/kafka-consumer-groups.sh --describe --group spark-kafka-source-73eb1215-9398-4d61-a437-ce20306f80ff-1221582759-driver-0 --members --bootstrap-server localhost:9092
  * -- ./bin/kafka-consumer-groups.sh --describe --group spark-kafka-source-73eb1215-9398-4d61-a437-ce20306f80ff-1221582759-driver-0 --bootstrap-server localhost:9092
  *
  * Other reads:
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-MicroBatchExecution.html
  * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-offsets-and-metadata-checkpointing.html
  */
object KafkaSourceFileSink {

  val appName = "KafkaIntegration"
  val queryNameKafkaIngest = "kafka-ingest"
  val queryNameRate3 = "rate-3s"
  val topicName = "example-topic,quickstart-events"
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

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")

      /**
        * "kafka.group.id" can also be set instead
        * Be careful about Kafka group-based authorization
        */
      .option("groupIdPrefix", "spark-kafka-ingest")
      .option("subscribe", topicName)
      //start consuming from the earliest. By default it will be the latest, which is to discard all history
      //.option("startingOffsets", "earliest")
      /**
        * If startingOffsets below are not configured, Spark will read from latest offset by default
        * If they are configured, validateTopicPartitions(https://github.com/apache/spark/blob/master/external/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaOffsetReaderConsumer.scala)
        * make sure that they must contain all TopicPartitions
        *
        * Newly discovered partitions during a query will start at earliest.
        */
      .option("startingOffsets", s"""{"example-topic":{"0":-2,"1":-2,"2":-2},"quickstart-events":{"0":-2}}""")
      //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries
      .load()

      /**
        * The original data frame loaded. See the row below as an example.
        * {
        * "key":"ZW1wMTAw",                                                                       //base64 encoded bytes
        * "value":"eyJlbXBfaWQiOjEwMCwiZmlyc3RfbmFtZSI6Iktlc2hhdiIsImxhc3RfbmFtZSI6IkxvZGhpIn0=", //base64 encoded bytes
        * "topic":"example-topic",
        * "partition":0,
        * "offset":5,
        * "timestamp":"2021-03-28T18:22:18.819-07:00",   //message produced time
        * "timestampType":0
        * }
        */
      // Another example for adding extra columns
      // https://stackoverflow.com/questions/46130191/why-do-id-and-runid-change-every-start-of-a-streaming-query
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json")

    /**
      * If the schema, from_json(df("json"), TestTopicSchema.employeeSchema), is not provided, the JSON will be ingested
      * as it is
      *
      * If the schema is provided, e.g.
      * df.select(df("key"), from_json(df("json"), TestTopicSchema.employeeSchema).as("data")),
      * then the following columns will not be included
      * 1. Columns where the types don't match the schema
      * 2. Extra columns that are not defined in the schema
      * 3. Missing columns defined in the schema but not in the payload
      *
      * Ref:
      * https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html
      */
    val kafkaDF = df.select(df("key"), from_json(df("json"), TestTopicSchema.employeeSchema).as("data"))
    //.select("key", "data.*")  //flatten the nested columns within data

    val kafkaIngestWriter: DataStreamWriter[Row] = kafkaDF.writeStream
      .queryName(queryNameKafkaIngest)
      //TODO try partition
      //.partitionBy("")
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

    // Sleep 2 seconds for waiting for an execution
    // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/StreamExecution.scala#L532
    Thread.sleep(2000)
    qKafkaIngest.explain(true)

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

object TestTopicSchema {
  val employeeSchema: StructType = StructType(Array(
    StructField("emp_id", IntegerType),
    StructField("first_name", StringType),
    StructField("last_name", StringType)
  ))
}