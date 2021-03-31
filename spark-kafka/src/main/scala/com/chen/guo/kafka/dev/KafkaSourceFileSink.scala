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
  * emp101:{"emp_id":101,"first_name":"Mike","last_name":"M"}   //duplicate any event here to create a skewed dataset
  * emp105:{"emp_id":105,"first_name":"Tree","last_name":"T"}
  *
  * Bad messages
  * emp102:{"emp_id":"102c","first_name":"BadIdType","last_name":"B"}
  * emp103:{"emp_id":103,"first_name":"ExtraField","last_name":"E", "age":33}
  * emp104:{"first_name":"MissingField","last_name":"M"}
  *
  * To send Kafka messages with headers, you need to install "brew install kafkacat"
  * https://github.com/edenhill/kafkacat
  * https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
  *
  * To produce:
  * echo 'emp106:{"emp_id":106,"first_name":"Tracy","last_name":"T"}' | kafkacat -P -b localhost:9092 -t example-topic -K: -H "header1=value1" -H "nullheader"
  * (header value will be serialized, but not the header key? That's why we have usecases like nullheader??)
  * echo 'emp107:{"emp_id":107,"first_name":"Tame","last_name":"T"}' | kafkacat -P -b localhost:9092 -t example-topic -K:
  * kafkacat -P -b localhost:9092 -t example-topic -l records.json
  *
  * To consume:
  * kafkacat -b localhost:9092 -C -t example-topic -f '\nHeader: %h\nKey (%K bytes): %k\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'
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
  * https://www.waitingforcode.com/apache-spark-structured-streaming/apache-kafka-source-structured-streaming-beyond-offsets/read
  */
object KafkaSourceFileSink {

  val appName = "KafkaIntegration"
  val queryNamePrefix = "kfkingest"
  val consumerGroup = "spark-kafka-ingest"

  val queryNameRate3 = "rate-3s"
  //[GroupIdPrefix, TopicNames, TopicPartition Starting Offsets]
  val kafkaSourceQuickStartEvents: Array[String] = Array(consumerGroup, "quickstart-events", s"""{"quickstart-events":{"0":-2}}""")
  val kafkaSourceExampleTopic: Array[String] = Array(consumerGroup, "example-topic", s"""{"example-topic":{"0":-2,"1":-2,"2":-2}}""")
  val kafkaSourceCombined: Array[String] = Array(consumerGroup, "example-topic,quickstart-events", s"""{"example-topic":{"0":-2,"1":-2,"2":-2},"quickstart-events":{"0":-2}}""")


  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName(appName)

      /**
        * In Spark 3.0 and before Spark uses KafkaConsumer for offset fetching which could cause infinite wait in the driver.
        * See one issue here: https://issues.apache.org/jira/browse/SPARK-28367
        *
        * The default value is true. Set to false allowing Spark to use new offset fetching mechanism using AdminClient
        * instead of KafkaConsumer.
        *
        * The {@link org.apache.kafka.clients.admin.KafkaAdminClient} will be created when set false
        *
        * ACL Check: The following ACLs are needed from driver perspective
        * - Topic resource describe operation
        * Since AdminClient in driver is not connecting to consumer group, group.id based authorization will not work anymore
        * (executors never done group based authorization).
        * Worth to mention executor side is behaving the exact same way like before (group prefix and override works).
        *
        * Ref: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#offset-fetching
        */
      .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", false)
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

    if (false) {
      //Add multiple streaming queries
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("groupIdPrefix", kafkaSourceQuickStartEvents(0))
        .option("subscribe", kafkaSourceQuickStartEvents(1))
        .option("startingOffsets", kafkaSourceQuickStartEvents(2))
        .load()
        .writeStream
        .queryName(getQueryName(kafkaSourceQuickStartEvents(1)))
        .format("json")
        .option("path", getLocalPath(Constant.OutputPath, getQueryName(kafkaSourceQuickStartEvents(1))))
        .option("checkpointLocation", getLocalPath(Constant.CheckpointLocation, getQueryName(kafkaSourceQuickStartEvents(1))))
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime("3 seconds"))
        .start()
    }

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")

      /**
        * "kafka.group.id" can also be set instead
        * ACL Check: Kafka group-based authorization
        */
      .option("groupIdPrefix", kafkaSourceExampleTopic(0))

      /**
        * Another option is to use "assign" mode, where you don't have to consume all partitions for a topic
        * for example
        * .option("assign", """{"example-topic":[0,2]}""")   //this is to just consume from partition 0 and 2
        * .option("startingOffsets", s"""{"example-topic":{"0":-2,"2":-2}}""")
        */
      .option("subscribe", kafkaSourceExampleTopic(1))
      //start consuming from the earliest. By default it will be the latest, which is to discard all history
      //.option("startingOffsets", "earliest")
      /**
        * If startingOffsets below are not configured, Spark will read from latest offset by default
        * If they are configured, validateTopicPartitions(https://github.com/apache/spark/blob/master/external/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaOffsetReaderConsumer.scala)
        * make sure that they must contain all TopicPartitions
        *
        * Note that Spark automatically detects newly added partitions. Newly discovered partitions during a query will
        * start at earliest.
        * To experiment:
        * 1. Begin with
        * $ bin/kafka-consumer-groups.sh --describe --group spark-kafka-ingest-374de33a-442e-49d6-b6c4-283338743416--68643605-driver-0 --bootstrap-server localhost:9092
        * -- Only 3 partitions are subscribed
        *
        * 2. Increase the topic partitions from 3 to 4
        * bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic example-topic --partitions 4
        *
        * 3. Check again
        * $ bin/kafka-consumer-groups.sh --describe --group spark-kafka-ingest-374de33a-442e-49d6-b6c4-283338743416--68643605-driver-0 --bootstrap-server localhost:9092
        * -- 4 partitions are subscribed
        * Even if the newly added partition has 0 events, a batch job will still be created and committed for detecting
        * the new partition. An empty JSON output file(part-00000-xxx.json) will be committed.
        * Before altering, the offset is {"example-topic":{"2":2,"1":3,"0":1}}
        * After altering, the offset is {"example-topic":{"2":2,"1":3,"3":0,"0":1}}
        */
      .option("startingOffsets", kafkaSourceExampleTopic(2))

      /**
        * startingOffsetsByTimestamp takes precedence over startingOffsets.
        *
        * When startingOffsetsByTimestamp is used, the timestamp will be firstly translated into offsets and kept in the
        * checkpoint/.../sources/0/0 file. Newly discovered partitions during a query will start at earliest.
        *
        * Example:
        * .option("startingOffsetsByTimestamp", s"""{"example-topic":{"0":1617163070574,"1":1617140874379,"2":1617153761857}}""")
        *
        * To experiment:
        * Run the command below to select a timestamp for each column
        * kafkacat -b localhost:9092 -C -t example-topic -f '\nHeader: %h\nKey (%K bytes): %k\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'
        *
        * Verify that the events ingested match the selection of the timestamp for each partition
        */
      //.option("startingOffsetsByTimestamp", s"""{"example-topic":{"0":1617163070574,"1":1617140874379,"2":1617153761857}}""")
      //.option("endingOffsets", "latest")  //ending offset cannot be set in streaming queries

      /**
        * KafkaOffsetRangeCalculator calculates offset ranges to process based on the from and until offsets, and the
        * configured `minPartitions`, then assign each task to a executor. Apache Spark will try to always allocate the
        * consumers reading given partition on the same executor so that the cached consumers can be reused and not evicted.
        * Therefore, it's okay to have more partitions than the number of Kafka topic partitions.
        *
        * If `minPartitions` is not set or is set less than or equal the number of `topicPartitions` that we're going to
        * consume, then we fall back to a 1-1 mapping of Spark tasks to Kafka partitions. If `numPartitions` is set
        * higher than the number of our `topicPartitions`, then we will split up the read tasks of the skewed partitions
        * to multiple Spark tasks.
        *
        * The number of Spark tasks will be approximately `numPartitions`. It can be less or more depending on rounding
        * errors or Kafka partitions that didn't receive any new data.
        *
        * To experiment:
        * Find an event and duplicate it several times in the kafka-console-producer.sh and restart the streaming
        * ingestion. You will find that 4 part-0000x- files will be generated even if there are only 3 topic partitions.
        */
      .option("minPartitions", 4)
      .option("includeHeaders", "true")
      .load()

      /**
        * The original data frame loaded. See the row below as an example.
        * {
        * "key":"ZW1wMTAw",                                                                       //base64 encoded bytes
        * "value":"eyJlbXBfaWQiOjEwMCwiZmlyc3RfbmFtZSI6Iktlc2hhdiIsImxhc3RfbmFtZSI6IkxvZGhpIn0=", //base64 encoded bytes
        * "topic":"example-topic",
        * "partition":0,
        * "offset":5,
        * "timestamp":"2021-03-28T18:22:18.819-07:00",   //It's a message CreateTime based on the timestampType
        *
        * //This timestampType comes from https://github.com/apache/spark/blob/master/external/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaSourceRDD.scala#L87
        * //which further references {@link org.apache.kafka.common.record.TimestampType}
        * //This is a cluster-wide configuration on the Kafka side, with the configuration key "log.message.timestamp.type"
        * //Read more at https://kafka.apache.org/documentation/#brokerconfigs_log.message.timestamp.type
        * "timestampType":0
        *
        * //"headers" is an optional field, it's included in the row only when "includeHeaders" is set to true
        * "headers":[{"key":"header1","value":"dmFsdWUx"},{"key":"nullheader"}]
        * }
        */
      // Another example for adding extra columns
      // https://stackoverflow.com/questions/46130191/why-do-id-and-runid-change-every-start-of-a-streaming-query
      .selectExpr("timestamp",
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as json",
        "CAST(headers AS ARRAY<STRUCT<key:STRING,value:STRING>>) as headers")

    /**
      * If the schema, from_json(df("json"), TestTopicSchema.employeeSchema), is not provided, the JSON will be
      * ingested as it is
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
    val kafkaDF = df.select(
      df("timestamp"),
      df("key"),
      from_json(df("json"), TestTopicSchema.employeeSchema).as("data"),
      //Find the header with name = "header1" and use its value as a column
      expr("filter(headers, header -> header.key == 'header1')[0].value").as("header1"))
    //.select("key", "data.*")  //flatten the nested columns within data, but physical plan doesn't look optimal

    val kafkaIngestWriter: DataStreamWriter[Row] = kafkaDF.writeStream
      .queryName(getQueryName(kafkaSourceExampleTopic(1)))
      //TODO try partition
      //.partitionBy("")
      .format("json")
      .option("path", getLocalPath(Constant.OutputPath, getQueryName(kafkaSourceExampleTopic(1))))
      .option("checkpointLocation", getLocalPath(Constant.CheckpointLocation, getQueryName(kafkaSourceExampleTopic(1))))
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

  def getQueryName(topicNames: String): String = s"${queryNamePrefix}-${topicNames}"

  def getLocalPath(rootDir: String, queryName: String): String = s"${rootDir}/${queryName}"

  /**
    * Stop queries based on a fixed schedule
    */
  def stopQueriesOption1(spark: SparkSession): Unit = {
    val map = spark.streams.active.map(x => (x.name, x)).toMap
    executor.schedule(new Runnable {
      override def run(): Unit = map(getQueryName(kafkaSourceExampleTopic(1))).stop()
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