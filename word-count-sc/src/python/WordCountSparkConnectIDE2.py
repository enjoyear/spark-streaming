from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, lit, desc
from pyspark.sql.types import ArrayType, StringType


def main(spark: SparkSession):
    # spark.addArtifacts(
    #     "/path/to/your_wheel-0.1-py3-none-any.whl",
    #     pyfile=True,
    # )

    # This is your closure. It will be serialized and run on the Python workers.
    def split_and_filter(line: str):
        if line is None:
            return []
        return [w for w in line.split() if w]

    split_and_filter_udf = udf(split_and_filter, ArrayType(StringType()))

    words = (
        spark.read
        .text("file:///etc/passwd")
        .select(
            explode(split_and_filter_udf(col("value"))).alias("word")
        )
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
    import sys
    print(f"Python version: {sys.version}")
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
