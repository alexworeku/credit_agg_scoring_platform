import os
from datetime import date as pydate

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.common.config import settings
from pipeline.common.io_utils import write_parquet_table
from pipeline.common.spark_utils import build_spark


def read_bronze_table(spark, table_name: str) -> DataFrame:
    return spark.read.parquet(str(settings.bronze_dir / table_name))


def json_col(raw_col: str, field: str):
    return F.get_json_object(F.col(raw_col), f"$.{field}")


def to_int(raw_col: str, field: str):
    return json_col(raw_col, field).cast("int")


def to_double(raw_col: str, field: str):
    return json_col(raw_col, field).cast("double")


def _reference_date_expr():
    configured = os.getenv("PIPELINE_REFERENCE_DATE")
    if configured:
        try:
            pydate.fromisoformat(configured)
        except ValueError as exc:
            raise ValueError(
                f"Invalid PIPELINE_REFERENCE_DATE='{configured}'. Expected YYYY-MM-DD."
            ) from exc
        return F.to_date(F.lit(configured))
    # Default anchor is the Bronze ingestion date for each row.
    return F.coalesce(F.to_date(F.col("ingestion_timestamp")), F.current_date())


def build_customers(customers_raw: DataFrame) -> DataFrame:
    return customers_raw.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("institution_id"),
        F.col("source_system"),
        json_col("raw_json", "CODE_GENDER").alias("gender"),
        F.floor(F.abs(to_int("raw_json", "DAYS_BIRTH")) / F.lit(365.25)).cast("int").alias("age"),
        to_double("raw_json", "AMT_INCOME_TOTAL").alias("income"),
        json_col("raw_json", "NAME_INCOME_TYPE").alias("employment_type"),
        json_col("raw_json", "NAME_EDUCATION_TYPE").alias("education"),
        json_col("raw_json", "NAME_HOUSING_TYPE").alias("housing_type"),
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_REGISTRATION")).alias("account_open_date"),
        F.col("record_source").alias("record_source"),
        F.current_timestamp().alias("created_at"),
    ).dropDuplicates(["customer_id", "institution_id"])


def build_loans(loans_raw: DataFrame) -> DataFrame:
    base = loans_raw.select(
        F.col("loan_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("institution_id"),
        F.col("source_system"),
        F.col("record_source"),
        F.col("raw_json"),
    )

    loan_type = F.coalesce(
        json_col("raw_json", "NAME_CONTRACT_TYPE"),
        json_col("raw_json", "CREDIT_TYPE"),
    )
    loan_amount = F.coalesce(
        to_double("raw_json", "AMT_CREDIT"),
        to_double("raw_json", "AMT_CREDIT_SUM"),
        to_double("raw_json", "AMT_APPLICATION"),
    )
    loan_status = F.coalesce(
        json_col("raw_json", "NAME_CONTRACT_STATUS"),
        json_col("raw_json", "CREDIT_ACTIVE"),
        F.when(F.col("record_source") == "application_train", F.lit("APPLICATION")).otherwise(F.lit(None)),
    )
    start_date = F.coalesce(
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_DECISION")),
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_CREDIT")),
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_REGISTRATION")),
    )
    end_date = F.coalesce(
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_LAST_DUE")),
        F.date_add(F.current_date(), to_int("raw_json", "DAYS_CREDIT_ENDDATE")),
    )
    days_past_due = F.greatest(
        F.coalesce(to_int("raw_json", "CREDIT_DAY_OVERDUE"), F.lit(0)),
        F.coalesce(to_int("raw_json", "SK_DPD"), F.lit(0)),
        F.coalesce(to_int("raw_json", "SK_DPD_DEF"), F.lit(0)),
    )

    default_flag = F.coalesce(
        to_int("raw_json", "TARGET"),
        F.when(to_int("raw_json", "CREDIT_DAY_OVERDUE") > 0, F.lit(1)).otherwise(F.lit(0)),
    )

    return (
        base.select(
            F.col("loan_id"),
            F.col("customer_id"),
            F.col("institution_id"),
            F.col("source_system"),
            loan_type.alias("loan_type"),
            loan_amount.alias("loan_amount"),
            loan_status.alias("loan_status"),
            start_date.alias("start_date"),
            end_date.alias("end_date"),
            days_past_due.alias("days_past_due"),
            default_flag.alias("default_flag"),
            F.current_timestamp().alias("created_at"),
        )
        .filter(F.col("loan_id").isNotNull())
        .dropDuplicates(["loan_id"])
    )


def build_loan_payments(loan_payments_raw: DataFrame) -> DataFrame:
    reference_date = _reference_date_expr()
    payment_date = F.date_add(reference_date, to_int("raw_json", "DAYS_ENTRY_PAYMENT"))
    days_late = F.greatest(
        F.coalesce(to_int("raw_json", "DAYS_ENTRY_PAYMENT"), F.lit(0))
        - F.coalesce(to_int("raw_json", "DAYS_INSTALMENT"), F.lit(0)),
        F.lit(0),
    )

    return (
        loan_payments_raw.select(
            F.col("payment_id"),
            F.col("loan_id"),
            F.col("customer_id").cast("long").alias("customer_id"),
            F.col("institution_id"),
            F.col("source_system"),
            to_double("raw_json", "AMT_PAYMENT").alias("payment_amount"),
            to_double("raw_json", "AMT_INSTALMENT").alias("scheduled_amount"),
            payment_date.alias("payment_date"),
            days_late.alias("days_late"),
            F.current_timestamp().alias("created_at"),
        )
        .filter(F.col("payment_id").isNotNull())
        .dropDuplicates(["payment_id"])
    )


def build_account_balances(account_balances_raw: DataFrame) -> DataFrame:
    months_balance = to_int("raw_json", "MONTHS_BALANCE")
    reference_date = _reference_date_expr()
    snapshot_date = F.add_months(reference_date, months_balance)

    status_num = F.when(json_col("raw_json", "STATUS").rlike("^[0-5]$"), json_col("raw_json", "STATUS").cast("int")).otherwise(
        F.lit(0)
    )

    days_past_due = F.greatest(
        F.coalesce(to_int("raw_json", "SK_DPD"), F.lit(0)),
        F.coalesce(to_int("raw_json", "SK_DPD_DEF"), F.lit(0)),
        status_num,
    )

    transformed = account_balances_raw.select(
        F.col("account_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("institution_id"),
        F.col("source_system"),
        to_double("raw_json", "AMT_BALANCE").alias("balance"),
        to_double("raw_json", "AMT_CREDIT_LIMIT_ACTUAL").alias("credit_limit"),
        days_past_due.alias("days_past_due"),
        snapshot_date.alias("snapshot_date"),
        F.current_timestamp().alias("created_at"),
    ).filter(F.col("account_id").isNotNull() & F.col("customer_id").isNotNull())

    informative_rows = (
        (F.coalesce(F.abs(F.col("balance")), F.lit(0.0)) > F.lit(0.0))
        | (F.coalesce(F.abs(F.col("credit_limit")), F.lit(0.0)) > F.lit(0.0))
        | (F.coalesce(F.col("days_past_due"), F.lit(0)) > F.lit(0))
    )

    return transformed.filter(informative_rows).dropDuplicates(["account_id", "snapshot_date"])


def build_transactions(loan_payments: DataFrame, account_balances: DataFrame) -> DataFrame:
    payment_transactions = loan_payments.select(
        F.col("payment_id").alias("transaction_id"),
        F.col("customer_id"),
        F.col("institution_id"),
        F.col("source_system"),
        F.lit("LOAN_PAYMENT").alias("transaction_type"),
        F.col("payment_date").alias("transaction_date"),
        F.col("payment_amount").alias("amount"),
        F.col("scheduled_amount").alias("expected_amount"),
        (F.col("payment_amount") - F.col("scheduled_amount")).alias("delta_amount"),
        F.when(F.coalesce(F.col("days_late"), F.lit(0)) > 0, F.lit("LATE")).otherwise(F.lit("ON_TIME")).alias("status"),
        F.col("loan_id").alias("reference_id"),
        F.col("days_late").alias("days_past_due"),
        F.col("created_at"),
    )

    balance_transactions = account_balances.select(
        F.concat_ws("::", F.lit("BAL"), F.col("account_id"), F.col("snapshot_date").cast("string")).alias("transaction_id"),
        F.col("customer_id"),
        F.col("institution_id"),
        F.col("source_system"),
        F.lit("BALANCE_SNAPSHOT").alias("transaction_type"),
        F.col("snapshot_date").alias("transaction_date"),
        F.col("balance").alias("amount"),
        F.col("credit_limit").alias("expected_amount"),
        F.lit(None).cast("double").alias("delta_amount"),
        F.when(F.coalesce(F.col("days_past_due"), F.lit(0)) > 0, F.lit("PAST_DUE")).otherwise(F.lit("CURRENT")).alias("status"),
        F.col("account_id").alias("reference_id"),
        F.col("days_past_due"),
        F.col("created_at"),
    )

    return payment_transactions.unionByName(balance_transactions).filter(F.col("transaction_id").isNotNull())


def main() -> None:
    spark = build_spark("credit-agg-silver-transform")

    customers_raw = read_bronze_table(spark, "customers_raw")
    loans_raw = read_bronze_table(spark, "loans_raw")
    loan_payments_raw = read_bronze_table(spark, "loan_payments_raw")
    account_balances_raw = read_bronze_table(spark, "account_balances_raw")

    customers = build_customers(customers_raw)
    loans = build_loans(loans_raw)
    loan_payments = build_loan_payments(loan_payments_raw)
    account_balances = build_account_balances(account_balances_raw)
    transactions = build_transactions(loan_payments, account_balances)

    write_parquet_table(customers, settings.silver_dir / "customers")
    write_parquet_table(loans, settings.silver_dir / "loans")
    write_parquet_table(loan_payments, settings.silver_dir / "loan_payments")
    write_parquet_table(account_balances, settings.silver_dir / "account_balances")
    write_parquet_table(transactions, settings.silver_dir / "transactions")

    spark.stop()


if __name__ == "__main__":
    main()
