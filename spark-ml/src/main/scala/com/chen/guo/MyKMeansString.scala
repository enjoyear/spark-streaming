package com.chen.guo

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MyKMeansString extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("MyKMeans")
    .getOrCreate()
  val sc = spark.sparkContext

  val mongoIds: List[String] = List(
    "589f06ce2289a051cbef8bbc",
    "589c567963193a503f3fe579",
    "589568ecd752f100975789cc",
    "589aa6638c9737029b03cfd8",
    "5890ef2054b3ac3c1ffac6d7",
    "589fa0a1f7cf5b08d7aaed38",
    "58956839293a79008b602841"
  )

  val mongoIdSchema: StructType = StructType(Array(
    StructField("id", StringType, nullable = false)
  ))
  val sourceRdd: RDD[Row] = sc.parallelize(mongoIds.map(x => Row(x)))
  val dfSourceIds = spark.createDataFrame(sourceRdd, mongoIdSchema)
  val minId: String = dfSourceIds.agg(min("id").as("minId")).collect()(0).getAs[String]("minId")
  val minIdVal = Integer.parseInt(minId.substring(0, 8), 16)

  val rdd: RDD[Row] = sc.parallelize(mongoIds.map(x => Row(Array(
    (Integer.parseInt(x.substring(0, 8), 16) - minIdVal) * 1.0)))
  )
  val featureSchema: StructType = StructType(Array(
    StructField("features", ArrayType(DoubleType, false), nullable = false)
  ))
  val df = spark.createDataFrame(rdd, featureSchema)

  df.show()

  // Train a k-means model.
  val kmeans = new KMeans().setK(3).setSeed(1L)
  val model: KMeansModel = kmeans.fit(df)

  println(s"Cluster Centers: ${model.clusterCenters.length}")
  model.clusterCenters.foreach(println)

  // Make predictions
  val predictions: DataFrame = model.transform(df)
  val ranges = predictions.select(predictions("features")(0).as("id"), predictions("prediction"))
    .groupBy("prediction")
    .agg(min("id").as("rangeMin"), max("id").as("rangeMax"))

  ranges.show()

  ranges.collect().foreach(x => println(
    s">>> My range is: " +
      s"${(x.getAs[Double]("rangeMin").toLong + minIdVal).toHexString} ~ " +
      s"${(x.getAs[Double]("rangeMax").toLong + minIdVal).toHexString}"))
}
