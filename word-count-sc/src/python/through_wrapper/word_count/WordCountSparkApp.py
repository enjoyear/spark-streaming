from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, length, count, lit, desc
import argparse
import sys
from typing import List, Optional
import logging
from lib.dataframe_utils import collect_print_dataframe


def _main(spark: SparkSession, args: argparse.Namespace):
    input_file = args.input_file_path
    logging.info(f"Running word count for {input_file}")

    words = (
        spark.read
        .text(input_file)
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

    collect_print_dataframe(result)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="the parser"
    )
    parser.add_argument("--input-file-path", required=True, help="The path to the input file")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    file_path = getattr(args, "input_file_path", None)
    if file_path is None or not file_path.startswith("file://"):
        raise ValueError(f"{file_path} must starts with ...")


def main(unittest_argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )

    argv = None
    if unittest_argv is None:  # production call
        if len(sys.argv) >= 1 and sys.argv[0] != "--":
            raise RuntimeError(
                "Usage: main.py [-- <args..>]."
                "The first argument must be '--' if you want to provide arguments"
            )
        argv = sys.argv[1:]
    else:  # testing call
        if len(unittest_argv) >= 1 and unittest_argv[0] != "--":
            raise RuntimeError(
                "Usage: main.py [-- <args..>]."
                "The first argument must be '--' if you want to provide arguments"
            )
        argv = unittest_argv[1:]

    args = parse_args(argv)
    validate_args(args)

    spark = SparkSession.builder.appName("WordCount-wheel").getOrCreate()
    try:
        _main(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
