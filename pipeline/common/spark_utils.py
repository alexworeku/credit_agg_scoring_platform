import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "96")
    max_partition_bytes = os.getenv("SPARK_MAX_PARTITION_BYTES", str(128 * 1024 * 1024))
    master = os.getenv("SPARK_MASTER", "local[12]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "12g")
    driver_max_result_size = os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "2g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", driver_memory)

    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", max_partition_bytes)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", driver_max_result_size)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
