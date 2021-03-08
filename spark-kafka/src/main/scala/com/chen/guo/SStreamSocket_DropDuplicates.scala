package com.chen.guo

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener}


/**
  * Ref:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication
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
object SStreamSocket_DropDuplicates extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredNetworkJoin")
    .getOrCreate()

  // https://kontext.tech/column/spark/457/tutorial-turn-off-info-logs-in-spark
  // info log is too much
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val lines: DataFrame = spark.readStream
    //Socket source should be used only for testing as this does not provide end-to-end fault-tolerance guarantees.
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val people: Dataset[Person] = lines.as[String].map(Person(_)).dropDuplicates("name")

  // Start running the query that prints the running counts to the console
  val query: StreamingQuery = people.writeStream
    .queryName("MyDedupQuery")
    .outputMode(OutputMode.Append())
    .option("numRows", 100) //show more than 20 rows by default
    .option("truncate", value = false) //To show the full column content
    .format("console")
    .start()

  println("Print active streams")
  println(query.status)
  spark.streams.active.foreach(x => println(s"StreamQuery Id: ${x.id}"))

  //  // This doesn't seem to work
  //  val service: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  //  service.schedule(() => new Runnable {
  //    override def run(): Unit = {
  //      println(query.status)
  //    }
  //  }, 5, TimeUnit.SECONDS)

  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println("Query started: " + queryStarted.id)
    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      println("Query made progress: " + queryProgress.progress)
    }
  })

  query.awaitTermination()
}

case class Person(name: String, age: Int)

object Person {
  def apply(row: String): Person = {
    val parts = row.split(",")
    Person(parts(0).trim(), Integer.valueOf(parts(1).trim()))
  }
}