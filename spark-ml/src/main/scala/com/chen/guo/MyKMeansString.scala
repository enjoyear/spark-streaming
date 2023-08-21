package com.chen.guo

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, max, min}
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
    "63ff2f4fcb72c100017bcd0c",
    "64b0f74de35c7700011441bf",
    "64b6e161efcecc0001763dca",
    "64b80e8468c1b8000184b16c",
    "64c4243653ef6a0001a3fd38",
    "64c81a11e137c100010e8952",
    "64c81c67461a230001415fe2",
    "64c820ee845c87000102f4f8",
    "64c837f0835d770001e9c423",
    "64c839e8962fab00010803e0"
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
    // the job will fail if not converting array to vector using array_to_vector and vector_to_array
    // error message will be: java.lang.ArrayIndexOutOfBoundsException: Index 6 out of bounds for length 6
    .withColumn("features", array_to_vector(col("features")))

  df.show()

  // Train a k-means model.
  val kmeans = new KMeans().setK(10).setSeed(1L)
  val model: KMeansModel = kmeans.fit(df)

  println(s"Cluster Centers: ${model.clusterCenters.length}")
  model.clusterCenters.foreach(println)

  // Make predictions
  val predictions: DataFrame = model.transform(df)
  val ranges = predictions.select(vector_to_array(predictions("features"))(0).as("id"), predictions("prediction"))
    .groupBy("prediction")
    .agg(min("id").as("rangeMin"), max("id").as("rangeMax"))

  ranges.show()

  ranges.collect().foreach(x => println(
    s">>> My range is: " +
      s"${(x.getAs[Double]("rangeMin").toLong + minIdVal).toHexString} ~ " +
      s"${(x.getAs[Double]("rangeMax").toLong + minIdVal).toHexString}"))
}
