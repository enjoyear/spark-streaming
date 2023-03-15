package com.chen.guo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, _}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructType, _}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

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
  * echo '1:{"topic_name": "cdc-1", "operation": "start"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-2", "operation": "start"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-3", "operation": "start"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  *
  * Send events to cancel streams:
  * echo '1:{"topic_name": "cdc-1", "operation": "cancel"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-2", "operation": "cancel"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  * echo '1:{"topic_name": "cdc-3", "operation": "cancel"}' | kcat -P -b localhost:9092 -t soon-tasks -K:
  *
  * Send CDC events:
  * echo '1:{"id": "s1", "val": 1}' | kcat -P -b localhost:9092 -t cdc-1 -K:
  * echo '1:{"id": "s2", "val": 2}' | kcat -P -b localhost:9092 -t cdc-2 -K:
  * echo '1:{"id": "s3", "val": 3}' | kcat -P -b localhost:9092 -t cdc-3 -K:
  *
  * Send a bad CDC event to trigger failure:
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
  spark.conf.set("spark.sql.streaming.stopTimeout", "60s")
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
        Thread.sleep(5000)
        val activeStreams = spark.streams.active.map(_.name)
        logger.info(s"${activeStreams.length} active streams: ${activeStreams.mkString(",")}")
      }
    }
  }).start()

  while (true) {
    val records: ConsumerRecords[String, String] = tasksConsumer.poll(java.time.Duration.ofMillis(1000))
    for (record: ConsumerRecord[String, String] <- records.asScala) {
      breakable {
        val taskEventKey = record.key()
        val taskEventVal = record.value()
        val streamTask: Map[String, String] = mapper.readValue(taskEventVal, classOf[Map[String, String]])

        try {
          logger.info(s"Got $streamTask")
          val topicName = streamTask("topic_name")
          val operation = streamTask("operation").toLowerCase()
          val streamName = "Stream-for-" + topicName

          operation match {
            case "start" =>
              if (spark.streams.active.map(_.name).toSet.contains(streamName)) {
                logger.warn(s"Skip adding the stream $streamName because it already exists")
                break // continue to process the next event
              }

              logger.info(s"Adding a new stream $streamName for $topicName...")
              new Thread(new AddStream(spark, topicName, streamName, 3),
                "StreamSubmission-%d %s ".format(System.currentTimeMillis(), streamName)).start()
            case "cancel" =>
              val streamsByName = spark.streams.active.map(s => (s.name, s)).toMap
              val streamToCancel: Option[StreamingQuery] = streamsByName.get(streamName)
              if (streamToCancel.isEmpty) {
                logger.warn(s"Skip canceling the stream $streamName because it doesn't exist")
                break // continue to process the next event
              }
              new Thread(new CancelStream(streamToCancel.get, unhandledTasksProducer, taskDLQTopicName, record),
                "StreamCancellation-%d %s ".format(System.currentTimeMillis(), streamName)).start()
          }
        } catch {
          case consumeException: Exception =>
            logger.error(s"Creating new stream failed for $streamTask with error: ${consumeException.getLocalizedMessage}")
            val record: ProducerRecord[String, String] = new ProducerRecord[String, String](taskDLQTopicName,
              taskEventKey, dlqEventWithReason(taskEventVal, consumeException.getLocalizedMessage))
            try {
              // Produce synchronously
              unhandledTasksProducer.send(record).get()
            } catch {
              case produceException: Exception =>
                logger.error(s"Failed to send unprocessed task $streamTask with error: ${produceException.getLocalizedMessage}")
            }
        }
      }
    }

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

  def dlqEventWithReason(origVal: String, reason: String): String = {
    val dlqPayload = Map(
      "orig_request" -> origVal,
      "failed_reason" -> reason
    )
    mapper.writeValueAsString(dlqPayload)
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
      logger.info("Starting stream %s".format(streamName))
      val query: StreamingQuery = streamWriter.start()
      try {
        // This is a blocking call
        query.awaitTermination()
        logger.info("Stream %s terminated or cancelled gracefully".format(streamName))
        return
      } catch {
        case t: Throwable =>
          logger.warn(s"Stream $streamName terminated with exception ${t.getLocalizedMessage}. Restarting it with remaining retries: $remaining", t)
      }
    }

    if (remaining < 0) {
      logger.warn(s"Stream $streamName terminated after exhausting all $retries retries.")
    }
  }
}

class CancelStream(streamingQuery: StreamingQuery, unhandledTasksProducer: KafkaProducer[String, String],
                   taskDLQTopicName: String, cancelRequest: ConsumerRecord[String, String]) extends Runnable {
  val logger: Logger = LogManager.getLogger(this.getClass)

  override def run(): Unit = {
    try {
      logger.info(s"Start canceling stream ${streamingQuery.name} with UUID ${streamingQuery.id} for run ${streamingQuery.runId}")
      streamingQuery.stop()
      logger.info(s"Active streams are ${KafkaDynamicStreams.spark.streams.active.map(_.name).mkString(",")}")
      logger.info(s"Successfully canceled stream ${streamingQuery.name} with UUID ${streamingQuery.id} for run ${streamingQuery.runId}")
    } catch {
      case t: Throwable =>
        logger.error(s"Cancel stream failed for ${streamingQuery.name} with UUID ${streamingQuery.id} for run ${streamingQuery.runId}: ${t.getLocalizedMessage}", t)
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String](taskDLQTopicName,
          cancelRequest.key(), KafkaDynamicStreams.dlqEventWithReason(cancelRequest.value(), t.getLocalizedMessage))
        try {
          // Produce synchronously
          unhandledTasksProducer.send(record).get()
        } catch {
          case produceException: Exception =>
            logger.error(s"Failed sending failed cancellation request ${cancelRequest.value()} with error: ${produceException.getLocalizedMessage}", produceException)
        }
    }
  }
}

class QueryListener2(sqm: StreamingQueryManager) extends StreamingQueryListener {
  val logger: Logger = LogManager.getLogger(this.getClass)

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logger.info(s">> >> >> Query ${queryStarted.name} started at ${queryStarted.timestamp}: id ${queryStarted.id}, run id ${queryStarted.runId}")
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    logger.info(s">> >> >> Query terminated: id ${queryTerminated.id}, run id ${queryTerminated.runId}, " +
      s"exception ${queryTerminated.exception.getOrElse("n/a")}")
  }

  // for monitoring purpose
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    logger.info(s"Query ${queryProgress.progress.name} made progress.")
  }
}