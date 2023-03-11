package com.chen.guo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

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
  * Send test events
  * echo '1:{"topic_name": "cdc_t1", "task_id": 1}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  */
object KafkaDynamicStreams extends App {
  val logger = LoggerFactory.getLogger(this.getClass)
  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("KafkaIntegration")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  // Must be placed before the DataStreamWriter.start(), otherwise onQueryStarted won't be called
  spark.streams.addListener(new QueryListener2(spark.streams))

  private val taskTopicName = "soon-tasks"
  private val taskDLQTopicName = "dlq-soon-tasks"
  val (tasksConsumer, unhandledTasksProducer) = createKafkaClients(taskTopicName)

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  while (true) {
    val records: ConsumerRecords[String, String] = tasksConsumer.poll(java.time.Duration.ofMillis(1000))
    records.forEach(record => {
      val taskEventKey = record.key()
      val taskEventVal = record.value()
      val streamTask: Map[String, String] = mapper.readValue(taskEventVal, classOf[Map[String, String]])

      try {
        logger.info(s"Got $streamTask")
        val topicName = streamTask("topic_name")
        val streamName = "Stream" + streamTask("task_id")
        logger.info(s">>>>>>>>>>>  adding new stream $topicName , $streamName <<<<<<<<<<<<<")
        // new Thread(new MyThread(spark, topicName, streamName)).start()
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

class MyThread(ss: SparkSession, topicName: String, streamName: String) extends Runnable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import ss.implicits._

  def run() {
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()

    val kafkaDF: Dataset[(String, String)] = df
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as val")
      .as[(String, String)]

    val query: StreamingQuery = kafkaDF
      .writeStream
      .queryName(streamName)
      .outputMode(OutputMode.Append())
      .option("numRows", 100) //show more than 20 rows by default
      .option("truncate", value = false) //To show the full column content
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()

    try {
      // This is a blocking call
      query.awaitTermination()
    } catch {
      case t: Throwable =>
        logger.info(s"Stream terminated. Got exception: $t. Stacktrace is ${t.getStackTrace.mkString("\n")}")
    }
  }

}

// start your runnable thread somewhere later in the code


class QueryListener2(sqm: StreamingQueryManager) extends StreamingQueryListener {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

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