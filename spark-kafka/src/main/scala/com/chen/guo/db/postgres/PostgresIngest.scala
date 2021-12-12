package com.chen.guo.db.postgres

import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def withPartitionColumn: DataFrame = {
    val reader = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$hostName:5432/$dbName")
      .option("user", userName)
      .option("password", password)
      .option("dbtable", s"(${query}) as A")
      .option("partitionColumn", "id")
      .option("lowerBound", 1086710) //1068019, 1086719
      .option("upperBound", 1086730) //1068030, 1101683
      .option("numPartitions", 5)
      .option("fetchSize", 10000)
      .option("driver", "org.postgresql.Driver")

    reader.load()
  }

  withPartitionColumn.write.mode("overwrite").csv(s"/tmp/postgres/csv")

}