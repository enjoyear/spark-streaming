package com.chen.guo

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

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

  df.show()

  // Trains a k-means model.
  val kmeans = new KMeans().setK(3).setSeed(1L)
  val model: KMeansModel = kmeans.fit(df)

  //  // Make predictions
  //  val predictions = model.transform(df)
  //
  //  // Evaluate clustering by computing Silhouette score
  //  val evaluator = new ClusteringEvaluator()
  //
  //  val silhouette = evaluator.evaluate(predictions)
  //  println(s"Silhouette with squared euclidean distance = $silhouette")

  // Shows the result.
  println(s"Cluster Centers: ${model.clusterCenters.length}")
  model.clusterCenters.foreach(println)
  println("Done")
}