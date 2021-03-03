package com.chen.guo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Run a Netcat as a data server by using
  *   nc -lk 9999
  */
object Fun extends App {
  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(10))
  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  ssc.sparkContext.setLogLevel("WARN")

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)

  // Split each line into words
  val words: DStream[String] = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate
}
