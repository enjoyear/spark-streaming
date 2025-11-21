from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, length, count, lit, desc


def main(spark: SparkSession):
    # Avoid Python closures being sent as UDFs when possible:
    # use pure SQL expressions instead of .rdd / map / flatMap with lambdas.
    words = (
        spark.read
        .text("file:///etc/passwd")
        .select(
            explode(
                split(col("value"), r"\s+")
            ).alias("word")
        )
        .where(length(col("word")) > 0)
    )

    result = (
        words.groupBy("word")
        .agg(count(lit(1)).alias("count"))
        .orderBy(desc("count"))
        .limit(10)
    )

    for row in result.collect():
        print(row)


if __name__ == "__main__":
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
