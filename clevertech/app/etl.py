from functools import lru_cache
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="ML Logs Transformer")
    spark = SparkSession(sc)
    return spark


def load_logs(logs_path: Path) -> DataFrame:
    """
    TODO(Part 1.1): Complete this method
    """
    # logs = []
    # logs_path = "app/logs.jsonl"
    logs = get_spark().read.json(logs_path)
    return get_spark().createDataFrame(
        logs,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("metricId", IntegerType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )


def load_experiments(experiments_path: Path) -> DataFrame:
    """
    TODO(Part 1.2): Complete this method
    """
    # experiments = []
    # experiments_path = "app/experiments.csv"
    experiments = get_spark().read.csv(experiments_path)
    return get_spark().createDataFrame(
        experiments,
        StructType(
            [StructField("expId", IntegerType()), StructField("expName", StringType())]
        ),
    )


def load_metrics() -> DataFrame:
    """
    TODO(Part 1.3): Complete this method
    """
    # metrics = []
    metrics = [
        {"metricId": 0, "metricName": "Loss"},
        {"metricId": 1, "metricName": "Accuracy"},
    ]
    return get_spark().createDataFrame(
        metrics,
        StructType(
            [
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
            ]
        ),
    )


def join_tables(
    logs: DataFrame, experiments: DataFrame, metrics: DataFrame
) -> DataFrame:
    """
    TODO(Part 2): Complete this method
    """
    # joined_tables = []
    joined_tables = logs.join(experiments, "expId", "left").join(metrics, "metricId", "left")
    return get_spark().createDataFrame(
        joined_tables,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )


def filter_late_logs(data: DataFrame, hours: int) -> DataFrame:
    """
    TODO(Part 3): Complete this method
    """
    # filtered_logs = []
    filtered_logs = data.filter(data.ingestedAt > hours)
    return get_spark().createDataFrame(
        filtered_logs,
        StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
        ),
    )


def calculate_experiment_final_scores(data: DataFrame) -> DataFrame:
    """
    TODO(Part 4): Complete this method
    """
    # scores = []
    scores = data.groupBy("expId", "metricId", "expName", "metricName").agg({"value": "max"}).alias("maxValue").agg({"value": "min"}).alias("minValue")
    return get_spark().createDataFrame(
        scores,
        StructType(
            [
                StructField("expId", IntegerType()),
                StructField("metricId", IntegerType()),
                StructField("expName", StringType()),
                StructField("metricName", StringType()),
                StructField("maxValue", FloatType()),
                StructField("minValue", FloatType()),
            ]
        ),
    )


def save(data: DataFrame, output_path: Path):
    """
    TODO(Part 5): Complete this method
    """
    data.write.csv(output_path)
