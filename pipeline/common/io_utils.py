import os
from pathlib import Path

from pyspark.sql import DataFrame


def write_parquet_table(df: DataFrame, output_path: Path, mode: str = "overwrite") -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_partitions = max(1, int(os.getenv("PIPELINE_WRITE_PARTITIONS", "8")))
    df.coalesce(write_partitions).write.mode(mode).parquet(str(output_path))
