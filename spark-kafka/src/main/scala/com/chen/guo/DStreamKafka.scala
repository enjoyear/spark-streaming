package com.chen.guo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Referenced https://www.qubole.com/blog/dstreams-vs-dataframes-two-flavors-of-spark-streaming/
  */
object DStreamKafka extends App {
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("DStreamKafka")
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
  val ssc = new StreamingContext(sc, batchInterval)

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

  val messages: DStream[Message] = stream
    // We get a bunch of metadata from Kafka like partitions, timestamps, etc. Only interested in message payload
    .map(record => record.value)
    // We use flatMap to handle errors
    // by returning an empty list (None) if we encounter an issue and a
    // list with one element if everything is ok (Some(_)).
    .flatMap(record => {
      // Deserializing JSON using built-in Scala parser and converting it to a Message case class
      JSON.parseFull(record).map(rawMap => {
        val map = rawMap.asInstanceOf[Map[String, String]]
        Message(map.get("platform").get, map.get("uid").get, map.get("key").get, map.get("value").get)
      })
    })

  // Cache DStream now, it'll speed up most of the operations below
  messages.cache()

  val batchesToRun = 10 // How many batches to run before terminating
  messages.foreachRDD {
    rdd =>
      val finishedBatchesCounter = FinishedBatchesCounter.getInstance(sc)
      println(s"--- Batch ${finishedBatchesCounter.count + 1} ---")
      println("Processed messages in this batch: " + rdd.count())

      if (finishedBatchesCounter.count >= batchesToRun - 1) {
        ssc.stop()
      } else {
        finishedBatchesCounter.add(1)
      }
  }

  messages.map(msg => (msg.platform, 1))
    .reduceByKey(_ + _) //use reduceByKey, avoid GroupByKey: https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
    .print()

  // Printing messages with 'weird' uids
  val weirdUidMessages = messages.filter(msg => msg.uid == "NULL" || msg.uid == "" || msg.uid == " " || msg.uid.length < 10)
  weirdUidMessages.print(20)
  weirdUidMessages.count().print()

  // TODO: catch more violations here using filtering on 'messages'

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