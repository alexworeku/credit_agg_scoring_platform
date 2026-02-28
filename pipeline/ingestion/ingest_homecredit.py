from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline.common.config import settings
from pipeline.common.io_utils import write_parquet_table
from pipeline.common.spark_utils import build_spark


def read_csv(spark, filename: str) -> DataFrame:
    path = settings.raw_data_dir / filename
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "DROPMALFORMED")
        .csv(str(path))
    )


def with_metadata(df: DataFrame, customer_col: str, raw_cols: list[str], extra_cols: dict[str, Column]) -> DataFrame:
    return df.select(
        F.lit(settings.source_system).alias("source_system"),
        F.lit(settings.institution_id).alias("institution_id"),
        F.col(customer_col).cast("long").alias("customer_id"),
        *[expr.alias(name) for name, expr in extra_cols.items()],
        F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("raw_json"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )


def build_customers_raw(application_train: DataFrame) -> DataFrame:
    return with_metadata(
        application_train,
        customer_col="SK_ID_CURR",
        raw_cols=application_train.columns,
        extra_cols={"record_source": F.lit("application_train")},
    )


def build_loans_raw(application_train: DataFrame, previous_application: DataFrame, bureau: DataFrame) -> DataFrame:
    app_loans = with_metadata(
        application_train,
        customer_col="SK_ID_CURR",
        raw_cols=application_train.columns,
        extra_cols={
            "loan_id": F.concat(F.lit("APP_"), F.col("SK_ID_CURR").cast("string")),
            "record_source": F.lit("application_train"),
        },
    )

    prev_loans = with_metadata(
        previous_application,
        customer_col="SK_ID_CURR",
        raw_cols=previous_application.columns,
        extra_cols={
            "loan_id": F.concat(F.lit("PREV_"), F.col("SK_ID_PREV").cast("string")),
            "record_source": F.lit("previous_application"),
        },
    )

    bureau_loans = with_metadata(
        bureau,
        customer_col="SK_ID_CURR",
        raw_cols=bureau.columns,
        extra_cols={
            "loan_id": F.concat(F.lit("BUR_"), F.col("SK_ID_BUREAU").cast("string")),
            "record_source": F.lit("bureau"),
        },
    )

    return app_loans.unionByName(prev_loans).unionByName(bureau_loans)


def build_loan_payments_raw(installments: DataFrame) -> DataFrame:
    return with_metadata(
        installments,
        customer_col="SK_ID_CURR",
        raw_cols=installments.columns,
        extra_cols={
            "payment_id": F.concat_ws(
                "_",
                F.lit("PAY"),
                F.col("SK_ID_PREV").cast("string"),
                F.col("NUM_INSTALMENT_VERSION").cast("string"),
                F.col("NUM_INSTALMENT_NUMBER").cast("string"),
            ),
            "loan_id": F.concat(F.lit("PREV_"), F.col("SK_ID_PREV").cast("string")),
            "record_source": F.lit("installments_payments"),
        },
    )


def build_account_balances_raw(credit_card: DataFrame, pos_cash: DataFrame, bureau_balance: DataFrame, bureau: DataFrame) -> DataFrame:
    cc_raw = with_metadata(
        credit_card,
        customer_col="SK_ID_CURR",
        raw_cols=credit_card.columns,
        extra_cols={
            "account_id": F.concat(F.lit("CC_"), F.col("SK_ID_PREV").cast("string")),
            "record_source": F.lit("credit_card_balance"),
        },
    )

    pos_raw = with_metadata(
        pos_cash,
        customer_col="SK_ID_CURR",
        raw_cols=pos_cash.columns,
        extra_cols={
            "account_id": F.concat(F.lit("POS_"), F.col("SK_ID_PREV").cast("string")),
            "record_source": F.lit("POS_CASH_balance"),
        },
    )

    bureau_map = bureau.select(
        F.col("SK_ID_BUREAU").cast("long").alias("SK_ID_BUREAU"),
        F.col("SK_ID_CURR").cast("long").alias("SK_ID_CURR"),
    )

    bureau_balance_enriched = bureau_balance.join(bureau_map, on="SK_ID_BUREAU", how="left")

    bureau_raw = with_metadata(
        bureau_balance_enriched,
        customer_col="SK_ID_CURR",
        raw_cols=bureau_balance_enriched.columns,
        extra_cols={
            "account_id": F.concat(F.lit("BUR_"), F.col("SK_ID_BUREAU").cast("string")),
            "record_source": F.lit("bureau_balance"),
        },
    )

    return cc_raw.unionByName(pos_raw).unionByName(bureau_raw)


def main() -> None:
    spark = build_spark("credit-agg-bronze-ingestion")

    application_train = read_csv(spark, "application_train.csv")
    previous_application = read_csv(spark, "previous_application.csv")
    installments_payments = read_csv(spark, "installments_payments.csv")
    credit_card_balance = read_csv(spark, "credit_card_balance.csv")
    pos_cash_balance = read_csv(spark, "POS_CASH_balance.csv")
    bureau = read_csv(spark, "bureau.csv")
    bureau_balance = read_csv(spark, "bureau_balance.csv")

    customers_raw = build_customers_raw(application_train)
    loans_raw = build_loans_raw(application_train, previous_application, bureau)
    loan_payments_raw = build_loan_payments_raw(installments_payments)
    account_balances_raw = build_account_balances_raw(
        credit_card_balance,
        pos_cash_balance,
        bureau_balance,
        bureau,
    )

    write_parquet_table(customers_raw, settings.bronze_dir / "customers_raw")
    write_parquet_table(loans_raw, settings.bronze_dir / "loans_raw")
    write_parquet_table(loan_payments_raw, settings.bronze_dir / "loan_payments_raw")
    write_parquet_table(account_balances_raw, settings.bronze_dir / "account_balances_raw")

    spark.stop()


if __name__ == "__main__":
    main()
