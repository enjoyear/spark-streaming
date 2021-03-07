package com.chen.guo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType}


/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-static-joins
  *
  * How to run:
  * Run a Netcat as a data server by using
  * nc -lk 9999
  *
  * Then enter rows like below
  * Jack, 12
  * Jason, 40
  * CannotBeJoined, 100
  */
object SStreamSocket_StreamStaticJoin extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredNetworkJoin")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val schema = new StructType()
    .add("name", StringType)
    .add("address", StringType)
  val addrData = Seq(Row("Aaron", "San Francisco"), Row("Jack", "New York"), Row("Jason", "Seattle"))
  val addrRdd: RDD[Row] = spark.sparkContext.parallelize(addrData, 1)
  val addrTable = spark.createDataFrame(addrRdd, schema)
  addrTable.foreach(println(_))


  val lines: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val people: Dataset[Person] = lines.as[String].map(Person(_))
  val joined = people.join(addrTable, Seq("name"), "left_outer")

  // Start running the query that prints the running counts to the console
  val query = joined.writeStream
    //.outputMode(OutputMode.Complete()) // Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets
    .outputMode(OutputMode.Update())
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .format("console")
    .start()

  query.awaitTermination()
}

case class Person(name: String, age: Int)

object Person {
  def apply(row: String): Person = {
    val parts = row.split(",")
    Person(parts(0).trim(), Integer.valueOf(parts(1).trim()))
  }
}