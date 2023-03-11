package com.chen.guo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, _}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructType, _}

import java.text.SimpleDateFormat
import java.util.Properties

/**
  * Create test topics:
  * bin/kafka-topics.sh --create --topic soon-tasks --bootstrap-server localhost:9092
  * bin/kafka-topics.sh --create --topic dlq-soon-tasks --bootstrap-server localhost:9092
  * bin/kafka-topics.sh --create --topic cdc-1 --bootstrap-server localhost:9092
  * bin/kafka-topics.sh --create --topic cdc-2 --bootstrap-server localhost:9092
  * bin/kafka-topics.sh --create --topic cdc-3 --bootstrap-server localhost:9092
  *
  * To Listen:
  * kcat -C -t dlq-soon-tasks -b localhost:9092
  *
  * Send events to create streams:
  * echo '1:{"topic_name": "cdc-1"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-2"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-3"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  *
  * Send CDC events:
  * echo '1:{"id": "s1", "val": 1}' | kcat -P -b localhost:9092 -t cdc-1 -K:
  * echo '1:{"id": "s2", "val": 2}' | kcat -P -b localhost:9092 -t cdc-2 -K:
  * echo '1:{"id": "s3", "val": 3}' | kcat -P -b localhost:9092 -t cdc-3 -K:
  *
  * trigger a failure:
  * echo '1:{"id": "s1", "val": 11}' | kcat -P -b localhost:9092 -t cdc-1 -K:
  */
object KafkaDynamicStreams extends App {
  val logger = LogManager.getLogger(this.getClass)
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")
  val udfWithException = new UDFWithException()
  spark.udf.register("throwExceptionIfLessThan10", (x: Int) => udfWithException.throwException(x))

  // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
  spark.streams.addListener(new QueryListener2(spark.streams))

  private val taskTopicName = "soon-tasks"
  private val taskDLQTopicName = "dlq-soon-tasks"
  val (tasksConsumer, unhandledTasksProducer) = createKafkaClients(taskTopicName)

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  new Thread(new Runnable {
    val logger: Logger = LogManager.getLogger(this.getClass)

    override def run(): Unit = {
      while (true) {
        Thread.sleep(2000)
        val activeStreams = spark.streams.active.map(_.name)
        logger.info(s"${activeStreams.length} active streams: ${activeStreams.mkString(",")}")
      }
    }
  }).start()

  while (true) {
    val records: ConsumerRecords[String, String] = tasksConsumer.poll(java.time.Duration.ofMillis(1000))
    records.forEach(record => {
      val taskEventKey = record.key()
      val taskEventVal = record.value()
      val streamTask: Map[String, String] = mapper.readValue(taskEventVal, classOf[Map[String, String]])

      try {
        logger.info(s"Got $streamTask")
        val topicName = streamTask("topic_name")
        val streamName = "Stream-for-" + topicName
        logger.info(s"Adding a new stream $streamName for $topicName...")
        new Thread(new AddStream(spark, topicName, streamName, 3)).start()
      } catch {
        case consumeException: Exception =>
          logger.error(s"Creating new stream failed for $streamTask with error: ${consumeException.getLocalizedMessage}")
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String](taskDLQTopicName, taskEventKey, taskEventVal)
          try {
            // Produce synchronously
            unhandledTasksProducer.send(record).get()
          } catch {
            case produceException: Exception =>
              logger.error(s"Failed to send unprocessed task $streamTask with error: ${produceException.getLocalizedMessage}")
          }
      }
    })

    try {
      // Consume synchronously
      tasksConsumer.commitSync()
    } catch {
      case commitException: Exception =>
        logger.error(s"Failed to commit for $taskTopicName with error: ${commitException.getLocalizedMessage}")
    }
  }


  //// Termination Implementation 2
  //  while (!spark.streams.active.isEmpty) {
  //    logger.info("Queries currently still active: " + spark.streams.active.map(x => x.name).mkString(","))
  //    try {
  //      //await any to terminate
  //      spark.streams.awaitAnyTermination()
  //    } catch {
  //      case e: StreamingQueryException =>
  //        logger.info(s"Query failed (${e.message}) due to ${e.cause.getStackTrace.mkString("\n")}")
  //        logger.info("Queries currently still active: " + spark.streams.active.map(x => x.name).mkString(","))
  //        spark.streams.resetTerminated()
  //
  //      case t: Throwable =>
  //        throw new RuntimeException(s"Stream status is ${spark.streams.active}", t)
  //    }
  //  }

  def createKafkaClients(consumeTopicName: String): (KafkaConsumer[String, String], KafkaProducer[String, String]) = {
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "SparkJobsConsumerGroup")
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val tasksConsumer = new KafkaConsumer[String, String](consumerProps)
    tasksConsumer.subscribe(java.util.Arrays.asList(consumeTopicName))

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 2)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val unhandledTasksProducer = new KafkaProducer[String, String](producerProps)

    (tasksConsumer, unhandledTasksProducer)
  }
}

class AddStream(ss: SparkSession, topicName: String, streamName: String, retries: Int) extends Runnable {
  val logger: Logger = LogManager.getLogger(this.getClass)

  val schema: StructType = StructType(Array(
    StructField("id", StringType),
    StructField("val", IntegerType)
  ))

  def run() {
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()

    import ss.implicits._

    val kafkaDF = df
      .select(col("key").cast("string"), from_json(col("value").cast("string"), schema).as("payload"))
      .select(col("key"),
        col("payload.id").as("id"),
        col("payload.val").as("val"))
      .selectExpr("key", "id", "throwExceptionIfLessThan10(val)")
      .as[(String, String, Int)]

    val streamWriter: DataStreamWriter[(String, String, Int)] = kafkaDF
      .writeStream
      .queryName(streamName)
      .outputMode(OutputMode.Append())
      .option("numRows", 100) //show more than 20 rows by default
      .option("truncate", value = false) //To show the full column content
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")

    var remaining = retries
    while (remaining >= 0) {
      remaining = remaining - 1
      val query: StreamingQuery = streamWriter.start()
      try {
        // This is a blocking call
        query.awaitTermination()
      } catch {
        case t: Throwable =>
          logger.warn(s"Stream $streamName terminated with exception $t: ${t.getStackTrace.mkString("\n")}. Restarting it with remaining retries: $remaining")
      }
    }

    logger.warn(s"Stream $streamName terminated after exhausting all $retries retries.")
  }
}

class QueryListener2(sqm: StreamingQueryManager) extends StreamingQueryListener {
  val logger: Logger = LogManager.getLogger(this.getClass)

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logger.info(s"Query ${queryStarted.name} started at ${queryStarted.timestamp}: id ${queryStarted.id}, run id ${queryStarted.runId}")
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    logger.info(s"Query terminated: id ${queryTerminated.id}, run id ${queryTerminated.runId}, " +
      s"exception ${queryTerminated.exception.getOrElse("n/a")}")
  }

  // for monitoring purpose
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    logger.info(s"${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(System.currentTimeMillis)}: Query ${queryProgress.progress.name} made progress. " +
      s"Time take for retrieving latestOffset: ${queryProgress.progress.durationMs.get("latestOffset")}")
  }
}