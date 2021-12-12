package com.chen.guo.db.postgres

import org.apache.datasketches.theta.{PairwiseSetOperations, UpdateSketch}
import org.apache.spark.api.java.function.ReduceFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import java.util


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

  val query = "SELECT id FROM transactions"

  val initialValue: ThetaSketchJavaSerializable = new ThetaSketchJavaSerializable();

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

  val df: DataFrame = reader.load()
  df.printSchema()

  print("Table size: " + df.count())

  // df.write.mode("overwrite").csv(s"/tmp/postgres/csv")

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

  println(s"Unique count: ${sketch.getEstimate}")  //Accurate is 1097901, Estimate is 1107253.3

}