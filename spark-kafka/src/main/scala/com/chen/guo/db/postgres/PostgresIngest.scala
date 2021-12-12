package com.chen.guo.db.postgres

import org.apache.datasketches.theta.{PairwiseSetOperations, UpdateSketch}
import org.apache.spark.api.java.function.ReduceFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import java.util
import java.util.Properties


object PostgresIngest extends App {
  val spark = SparkSession
    .builder()
    .appName("Postgres")
    .master("local[6]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val hostName = System.getenv("POSTGRES_HOST_NAME")
  val dbName = System.getenv("POSTGRES_DB_NAME")
  val userName = System.getenv("POSTGRES_USER_NAME")
  val password = System.getenv("POSTGRES_PASSWORD")

  val query = "SELECT id, idem, currency FROM transactions"

  val initialValue: ThetaSketchJavaSerializable = new ThetaSketchJavaSerializable();

  val df = getDFByEvenPartition() // Full data
  // val df = getDFByCustomPartition() //Not implemented as loading full data

  private val quantiles: Array[Double] = df.stat.approxQuantile("id", Array(0, 0.25, 0.5, 0.75, 1), 0.05)

  /**
    * With relativeError 0: approxQuantile at PostgresIngest.scala:32, took 9.876182 s
    * Quantiles: 6.0,274743.0,549269.0,823945.0,1101894.0            <-- This is deterministic
    *
    * With relativeError 0.05: approxQuantile at PostgresIngest.scala:32, took 8.163279 s
    * Quantiles: 6.0,254664.0,540691.0,831862.0,1101895.0            <-- This is approximate
    *
    * With relativeError 0.1: approxQuantile at PostgresIngest.scala:32, took 7.256949 s
    * Quantiles: 6.0,200234.0,460821.0,732239.0,1101894.0            <-- This is approximate
    */
  println("Quantiles: " + quantiles.mkString(",")) // 6.0,274743.0,549269.0,823945.0,1101894.0

  df.printSchema()
  println("Table accurate size: " + df.count())
  df.write.mode("overwrite").csv(s"/tmp/postgres/csv")

  estimateDistinctCount()

  private def estimateDistinctCount(): Unit = {
    val mappedData: Dataset[ThetaSketchJavaSerializable] = df.mapPartitions((it: util.Iterator[Row]) => {
      val sketch = new ThetaSketchJavaSerializable
      while (it.hasNext) {
        sketch.update(it.next.get(0).toString)
      }
      util.Arrays.asList(sketch).iterator
    }, Encoders.javaSerialization(classOf[ThetaSketchJavaSerializable]))

    val sketch: ThetaSketchJavaSerializable = mappedData.reduce(new ReduceFunction[ThetaSketchJavaSerializable]() {
      @throws[Exception]
      override def call(sketch1: ThetaSketchJavaSerializable, sketch2: ThetaSketchJavaSerializable): ThetaSketchJavaSerializable = {
        if (sketch1.getSketch == null && sketch2.getSketch == null)
          return new ThetaSketchJavaSerializable(UpdateSketch.builder.build.compact)
        if (sketch1.getSketch == null)
          return sketch2
        if (sketch2.getSketch == null)
          return sketch1
        val compactSketch1 = sketch1.getCompactSketch
        val compactSketch2 = sketch2.getCompactSketch
        new ThetaSketchJavaSerializable(PairwiseSetOperations.union(compactSketch1, compactSketch2))
      }
    })
    println(s"Unique count: ${sketch.getEstimate}") //Accurate is 1097901, Estimate is 1107253.3
  }

  private def getDFByEvenPartition(): DataFrame = {
    // Use JDBC as a source
    // Read data as DataFrame: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val reader = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$hostName:5432/$dbName")
      .option("user", userName)
      .option("password", password)
      .option("dbtable", s"(${query}) as src_table")
      .option("partitionColumn", "id")
      .option("lowerBound", 6) //1068019
      .option("upperBound", 1101675) //1068030
      .option("numPartitions", 5)
      .option("fetchSize", 10000)
      .option("driver", "org.postgresql.Driver")

    reader.load()
  }

  private def getDFByCustomPartition(): DataFrame = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", userName)
    connectionProperties.put("password", password)
    connectionProperties.put("fetchSize", "10000")

    val predicates = Array(
      1068001 -> 1068021,
      1068021 -> 1068031,
      1068031 -> 1068040,
    ).map {
      case (start, end) =>
        s"id >= $start AND id <$end"
    }

    // Read data as JdbcRDD
    val df: DataFrame = spark.read.jdbc(
      url = s"jdbc:postgresql://$hostName:5432/$dbName",
      table = s"(${query}) as src_table",
      predicates = predicates,
      connectionProperties = connectionProperties
    )

    df
  }
}