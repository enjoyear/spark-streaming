package com.chen.guo

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MyKMeans extends App {
  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("MyKMeans")
    .getOrCreate()
  val sc = spark.sparkContext

  val rows: List[Row] = List(1, 2, 3, 4, 50, 51, 53, 58, 100, 102, 105).map(x => Row(Array(x * 1.0)))
  val rdd: RDD[Row] = sc.parallelize(rows)

  val table1KeySchema: StructType = StructType(Array(
    StructField("features", ArrayType(DoubleType, false), nullable = false)
  ))

  val table1Schema: StructType = StructType(table1KeySchema.fields)
  val df = spark.createDataFrame(rdd, table1Schema)
    // the input to KMeans should be a vector
    .withColumn("features", array_to_vector(col("features")))

  df.show()

  // Train a k-means model.
  val kmeans = new KMeans().setK(3).setSeed(1L)
  val model: KMeansModel = kmeans.fit(df)

  println(s"Cluster Centers: ${model.clusterCenters.length}")
  model.clusterCenters.foreach(println)

  // Make predictions
  val predictions: DataFrame = model.transform(df)
  predictions.select(vector_to_array(predictions("features"))(0).as("id"), predictions("prediction"))
    .groupBy("prediction")
    .agg(min("id").as("rangeMin"), max("id").as("rangeMax"))
    .show
}