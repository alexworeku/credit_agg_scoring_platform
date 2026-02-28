from pipeline.common.config import settings
from pipeline.common.io_utils import write_parquet_table
from pipeline.common.spark_utils import build_spark
from pipeline.gold.feature_logic import build_customer_credit_features


def read_silver_table(spark, table_name: str):
    return spark.read.parquet(str(settings.silver_dir / table_name))


def main() -> None:
    spark = build_spark("credit-agg-gold-feature-engineering")

    customers = read_silver_table(spark, "customers")
    loans = read_silver_table(spark, "loans")
    transactions = read_silver_table(spark, "transactions")

    features = build_customer_credit_features(
        customers=customers,
        loans=loans,
        transactions=transactions,
    )

    write_parquet_table(features, settings.gold_dir / "customer_credit_features")
    spark.stop()


if __name__ == "__main__":
    main()
