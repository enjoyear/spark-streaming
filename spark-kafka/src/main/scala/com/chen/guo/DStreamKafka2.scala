package com.chen.guo

import com.chen.guo.constant.Constant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * See set up at [[com.chen.guo.kafka.KafkaSourceFileSink]]
  */
object DStreamKafka2 extends App {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("DStreamKafka")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val topics = Array("example-topic")
  val ssc = new StreamingContext(sc, Seconds(5))
  ssc.checkpoint(Constant.CheckpointLocation + "/DStreamKafka2")

  val groupId = s"stream-checker-v${scala.util.Random.nextInt.toString}"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

  val messages: DStream[Example] = stream
    // We get a bunch of metadata from Kafka like partitions, timestamps, etc. Only interested in message payload
    .map(record => record.value)
    // We use flatMap to handle errors
    // by returning an empty list (None) if we encounter an issue and a
    // list with one element if everything is ok (Some(_)).
    .flatMap(record => {
      // Deserializing JSON using built-in Scala parser and converting it to a Message case class
      JSON.parseFull(record).map(rawMap => {
        val map = rawMap.asInstanceOf[Map[String, Object]]
        println("Got map: " + map)
        Example(map.getOrElse("emp_id", "n/a").toString, map.getOrElse("first_name", "n/a").toString, map.getOrElse("last_name", "n/a").toString)
      })
    })

  messages.print()

  // Cache DStream now, it'll speed up most of the operations below
  messages.cache()

  messages.foreachRDD {
    rdd: RDD[Example] =>
      println("Number of messages in this batch: " + rdd.count())
      rdd.foreach(println)
  }

  messages.map((msg: Example) => (msg.lastName, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()
}

case class Example(empId: String, firstName: String, lastName: String)