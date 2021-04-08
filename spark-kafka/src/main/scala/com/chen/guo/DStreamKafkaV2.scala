package com.chen.guo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, _}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Referenced https://www.qubole.com/blog/dstreams-vs-dataframes-two-flavors-of-spark-streaming/
  */
object DStreamKafkaV2 extends App {
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("DStreamKafkaV2")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val r = scala.util.Random
  // Generate a new Kafka Consumer group id every run
  val groupId = s"stream-checker-v${r.nextInt.toString}"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "kafka:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    // remove this if your Kafka doesn't use SSL
    "security.protocol" -> "SSL",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topic-to-consume")
  val batchInterval = Seconds(5)

  // Message schema (for JSON objects)
  val schema = StructType(
    StructField("platform", StringType, false) ::
      StructField("uid", StringType, false) ::
      StructField("key", StringType, false) ::
      StructField("value", StringType, false) :: Nil
  )

  val ssc = new StreamingContext(sc, batchInterval)
  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)
  )

  // We get a bunch of metadata from Kafka like partitions, timestamps, etc. Only interested in message payload
  val messages: DStream[String] = stream.map(record => record.value)

  val batchesToRun = 10
  // This turns accumulated batch of messages into RDD
  messages.foreachRDD { rdd: RDD[String] =>
    // Now we want to turn RDD into DataFrame
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    // Following 2 lines do all the magic for transforming String RDD into JSON DataFrame
    // *** Is this section below done by Spark Structured Streaming internally??  ***
    val rawDF = rdd.toDF("msg")
    val df = rawDF.select(from_json($"msg", schema) as "data").select("data.*")

    // Cache DataFrame now, it'll speed up most of the operations below
    df.cache()

    // We can create a view and use Spark SQL for queries
    //df.createOrReplaceTempView("msgs")
    //spark.sql("select count(*) from msgs where uid = '0'").show()

    val finishedBatchesCounter = FinishedBatchesCounter.getInstance(sc)

    println(s"--- Batch ${finishedBatchesCounter.count + 1} ---")
    println("Processed messages in this batch: " + df.count())

    println("All platforms:")
    df.groupBy("platform").count().show()

    println("Weird user ids:")
    val weirdUidDF = df.filter("uid is NULL OR uid = '' OR uid = ' ' OR   length(uid) < 10")
    weirdUidDF.show(20, false)
    println("Total: " + weirdUidDF.count())

    // TODO: catch more violations here using filtering on 'df'

    // Counting batches and terminating after 'batchesToRun'
    if (finishedBatchesCounter.count >= batchesToRun - 1) {
      ssc.stop()
    } else {
      finishedBatchesCounter.add(1)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}

// Counter for the number of batches. The job will stop after it reaches 'batchesToRun' value
// Looks ugly, but this is what documentation uses as an example ¯\_(ツ)_/¯
object FinishedBatchesCounter {
  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("FinishedBatchesCounter")
        }
      }
    }
    instance
  }
}

case class Message(platform: String, uid: String, key: String, value: String)