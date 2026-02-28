from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def safe_divide(numerator, denominator):
    return F.when(denominator.isNotNull() & (denominator != 0), numerator / denominator).otherwise(F.lit(None))


def build_customer_credit_features(customers: DataFrame, loans: DataFrame, transactions: DataFrame) -> DataFrame:
    customer_universe = (
        customers.select("customer_id")
        .unionByName(loans.select("customer_id"))
        .unionByName(transactions.select("customer_id"))
        .dropna()
        .dropDuplicates(["customer_id"])
    )

    customer_meta = customers.groupBy("customer_id").agg(
        F.when(F.countDistinct("institution_id") > 1, F.lit("multi"))
        .otherwise(F.first("institution_id", ignorenulls=True))
        .alias("institution_id"),
        F.when(F.countDistinct("source_system") > 1, F.lit("multi"))
        .otherwise(F.first("source_system", ignorenulls=True))
        .alias("source_system"),
        F.max("income").alias("income"),
    )

    loan_status_upper = F.upper(F.coalesce(F.col("loan_status"), F.lit("")))
    loan_agg = loans.groupBy("customer_id").agg(
        F.countDistinct("loan_id").alias("total_loans"),
        F.sum(F.when(loan_status_upper.rlike("ACTIVE|APPROVED"), F.lit(1)).otherwise(F.lit(0))).alias("active_loans"),
        F.sum(F.when(loan_status_upper.rlike("CLOSED|COMPLETED"), F.lit(1)).otherwise(F.lit(0))).alias("closed_loans"),
        F.max("days_past_due").alias("loan_max_days_past_due"),
        F.avg("days_past_due").alias("loan_avg_days_past_due"),
        F.sum(F.when(loan_status_upper.rlike("ACTIVE|APPROVED"), F.col("loan_amount")).otherwise(F.lit(0.0))).alias(
            "active_loan_debt"
        ),
        F.max("default_flag").alias("default_flag"),
    )

    payments = transactions.filter(F.col("transaction_type") == "LOAN_PAYMENT")
    payment_agg = payments.groupBy("customer_id").agg(
        F.countDistinct("transaction_id").alias("total_payments"),
        F.sum(F.when(F.coalesce(F.col("days_past_due"), F.lit(0)) > 0, F.lit(1)).otherwise(F.lit(0))).alias("late_payments"),
    )

    balances = transactions.filter(F.col("transaction_type") == "BALANCE_SNAPSHOT")
    balance_agg = balances.groupBy("customer_id").agg(
        F.countDistinct("reference_id").alias("total_accounts"),
        F.datediff(F.current_date(), F.min("transaction_date")).alias("account_age_days"),
        F.avg("amount").alias("avg_balance"),
        F.max("amount").alias("max_balance"),
        F.min("amount").alias("min_balance"),
        F.avg("expected_amount").alias("avg_credit_limit"),
        F.stddev_samp("amount").alias("account_stability"),
        F.max("days_past_due").alias("account_max_days_past_due"),
        F.avg("days_past_due").alias("account_avg_days_past_due"),
        F.sum(F.when(F.col("amount") > 0, F.col("amount")).otherwise(F.lit(0.0))).alias("account_positive_balance"),
    )

    return (
        customer_universe.join(customer_meta, on="customer_id", how="left")
        .join(loan_agg, on="customer_id", how="left")
        .join(payment_agg, on="customer_id", how="left")
        .join(balance_agg, on="customer_id", how="left")
        .select(
            F.col("customer_id").cast("long").alias("customer_id"),
            F.coalesce(F.col("institution_id"), F.lit("unknown")).alias("institution_id"),
            F.coalesce(F.col("source_system"), F.lit("unknown")).alias("source_system"),
            F.coalesce(F.col("total_accounts"), F.lit(0)).cast("int").alias("total_accounts"),
            F.coalesce(F.col("account_age_days"), F.lit(0)).cast("int").alias("account_age_days"),
            F.coalesce(F.col("avg_balance"), F.lit(0.0)).alias("avg_balance"),
            F.coalesce(F.col("max_balance"), F.lit(0.0)).alias("max_balance"),
            F.coalesce(F.col("min_balance"), F.lit(0.0)).alias("min_balance"),
            F.coalesce(F.col("total_loans"), F.lit(0)).cast("int").alias("total_loans"),
            F.coalesce(F.col("active_loans"), F.lit(0)).cast("int").alias("active_loans"),
            F.coalesce(F.col("closed_loans"), F.lit(0)).cast("int").alias("closed_loans"),
            F.coalesce(F.col("total_payments"), F.lit(0)).cast("int").alias("total_payments"),
            F.coalesce(F.col("late_payments"), F.lit(0)).cast("int").alias("late_payments"),
            F.coalesce(safe_divide(F.col("late_payments"), F.col("total_payments")), F.lit(0.0)).alias("late_payment_ratio"),
            F.coalesce(F.greatest(F.col("loan_max_days_past_due"), F.col("account_max_days_past_due")), F.lit(0))
            .cast("int")
            .alias("max_days_past_due"),
            F.coalesce(
                (F.coalesce(F.col("loan_avg_days_past_due"), F.lit(0.0)) + F.coalesce(F.col("account_avg_days_past_due"), F.lit(0.0)))
                / F.when(
                    F.col("loan_avg_days_past_due").isNotNull() & F.col("account_avg_days_past_due").isNotNull(), F.lit(2.0)
                ).otherwise(F.lit(1.0)),
                F.lit(0.0),
            ).alias("avg_days_past_due"),
            (
                F.coalesce(F.col("active_loan_debt"), F.lit(0.0))
                + F.coalesce(F.col("account_positive_balance"), F.lit(0.0))
            ).alias("total_debt"),
            F.coalesce(
                safe_divide(
                    F.coalesce(F.col("active_loan_debt"), F.lit(0.0)) + F.coalesce(F.col("account_positive_balance"), F.lit(0.0)),
                    F.col("income"),
                ),
                F.lit(0.0),
            ).alias("debt_to_income_ratio"),
            F.coalesce(safe_divide(F.col("avg_balance"), F.col("avg_credit_limit")), F.lit(0.0)).alias("credit_utilization"),
            F.coalesce(F.col("account_stability"), F.lit(0.0)).alias("account_stability"),
            F.coalesce(F.col("default_flag"), F.lit(0)).cast("int").alias("default_flag"),
            F.current_timestamp().alias("created_at"),
        )
    )
