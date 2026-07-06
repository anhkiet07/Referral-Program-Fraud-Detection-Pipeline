"""
referral_pipeline.py
---------------------
Springer Capital take-home test - Data Engineer Intern.

End-to-end pipeline that:
  1. Loads all 7 raw referral-program CSV tables.
  2. Cleans them (null normalization, dtype casting, de-duplication).
  3. Joins everything into a single referral-detail dataset.
  4. Converts timestamps from UTC to each record's local timezone.
  5. Applies the business rules to flag suspicious / invalid referral
     rewards in a new `is_business_logic_valid` column.
  6. Writes the final report as a single CSV file.

Usage:
    python src/referral_pipeline.py --input-dir data/raw --output-dir output/report
"""

import argparse
import os
import shutil

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils import normalize_nulls, initcap_except


# --------------------------------------------------------------------------- #
# 1. Data Loading
# --------------------------------------------------------------------------- #
def load_data(spark: SparkSession, input_dir: str) -> dict:
    """Load every raw CSV into a DataFrame, keyed by table name."""
    tables = {}
    file_map = {
        "lead_log": "lead_log.csv",
        "paid_transactions": "paid_transactions.csv",
        "referral_rewards": "referral_rewards.csv",
        "user_logs": "user_logs.csv",
        "user_referral_logs": "user_referral_logs.csv",
        "user_referral_statuses": "user_referral_statuses.csv",
        "user_referrals": "user_referrals.csv",
    }
    for name, filename in file_map.items():
        path = os.path.join(input_dir, filename)
        df = spark.read.csv(path, header=True, inferSchema=True)
        tables[name] = normalize_nulls(df)
    return tables


# --------------------------------------------------------------------------- #
# 2. Data Cleaning
# --------------------------------------------------------------------------- #
def clean_data(tables: dict) -> dict:
    """Fix data types and remove duplicate rows in reference/dimension
    tables so that later joins don't fan-out (Test Instruction: 'Join
    Tables ... make sure there is no duplicate')."""

    # --- user_logs: repeated identical rows per user_id -> keep one ---
    user_logs = tables["user_logs"].withColumn(
        "is_deleted", F.col("is_deleted").cast("string").cast("boolean")
    )
    w = Window.partitionBy("user_id").orderBy(F.col("id").asc())
    user_logs = (
        user_logs.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    tables["user_logs"] = user_logs

    # --- lead_log: repeated rows per lead_id -> keep the earliest record ---
    w = Window.partitionBy("lead_id").orderBy(F.col("id").asc())
    lead_log = (
        tables["lead_log"]
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    tables["lead_log"] = lead_log

    # --- user_referral_logs: multiple status-update rows per referral. ---
    # Collapse to a single row per user_referral_id: was the reward EVER
    # granted, and if so, when.
    is_granted_bool = F.upper(F.col("is_reward_granted").cast("string")) == F.lit("TRUE")
    user_referral_logs = tables["user_referral_logs"].withColumn(
        "is_reward_granted_bool", is_granted_bool
    )
    user_referral_logs = user_referral_logs.groupBy("user_referral_id").agg(
        F.max("is_reward_granted_bool").alias("is_reward_granted"),
        F.max(F.when(F.col("is_reward_granted_bool"), F.col("created_at"))).alias("reward_granted_at"),
        F.max(F.when(F.col("is_reward_granted_bool"), F.col("source_transaction_id"))).alias(
            "source_transaction_id"
        ),
    )
    tables["user_referral_logs"] = user_referral_logs

    # --- referral_rewards: parse "10 days" -> integer 10 ---
    tables["referral_rewards"] = tables["referral_rewards"].withColumn(
        "num_reward_days", F.regexp_extract(F.col("reward_value"), r"(\d+)", 1).cast("int")
    )

    return tables


# --------------------------------------------------------------------------- #
# 3. Data Processing: joins, timezone adjustment, string formatting
# --------------------------------------------------------------------------- #
def process_data(tables: dict):
    referrals = tables["user_referrals"]
    referrer = tables["user_logs"].select(
        F.col("user_id").alias("referrer_id"),
        F.col("name").alias("referrer_name"),
        F.col("phone_number").alias("referrer_phone_number"),
        F.col("homeclub").alias("referrer_homeclub"),
        F.col("timezone_homeclub").alias("referrer_timezone"),
        F.col("membership_expired_date").alias("referrer_membership_expired_date"),
        F.col("is_deleted").alias("referrer_is_deleted"),
    )
    leads = tables["lead_log"].select(
        F.col("lead_id"),
        F.col("source_category").alias("lead_source_category"),
    )
    statuses = tables["user_referral_statuses"].select(
        F.col("id").alias("user_referral_status_id"),
        F.col("description").alias("referral_status"),
    )
    rewards = tables["referral_rewards"].select(
        F.col("id").alias("referral_reward_id"),
        F.col("num_reward_days"),
    )
    transactions = tables["paid_transactions"]
    reward_logs = tables["user_referral_logs"]

    df = (
        referrals
        .join(referrer, on="referrer_id", how="left")
        .join(leads, referrals["referee_id"] == leads["lead_id"], how="left")
        .join(statuses, on="user_referral_status_id", how="left")
        .join(rewards, on="referral_reward_id", how="left")
        .join(transactions, on="transaction_id", how="left")
        .join(
            reward_logs,
            referrals["referral_id"] == reward_logs["user_referral_id"],
            how="left",
        )
    )

    # --- Source Category (per the CASE WHEN given in the spec) ---
    df = df.withColumn(
        "referral_source_category",
        F.when(F.col("referral_source") == "User Sign Up", F.lit("Online"))
        .when(F.col("referral_source") == "Draft Transaction", F.lit("Offline"))
        .when(F.col("referral_source") == "Lead", F.col("lead_source_category"))
        .otherwise(F.lit(None)),
    )

    # --- Time Adjustment: UTC -> local time ---
    # referral/updated/reward-granted timestamps belong to the referrer, so
    # they use the referrer's homeclub timezone. Transaction timestamps
    # carry their own timezone column.
    df = (
        df.withColumn(
            "referral_at_local",
            F.from_utc_timestamp(F.col("referral_at"), F.coalesce(F.col("referrer_timezone"), F.lit("Asia/Jakarta"))),
        )
        .withColumn(
            "updated_at_local",
            F.from_utc_timestamp(F.col("updated_at"), F.coalesce(F.col("referrer_timezone"), F.lit("Asia/Jakarta"))),
        )
        .withColumn(
            "reward_granted_at_local",
            F.when(
                F.col("reward_granted_at").isNotNull(),
                F.from_utc_timestamp(
                    F.col("reward_granted_at"), F.coalesce(F.col("referrer_timezone"), F.lit("Asia/Jakarta"))
                ),
            ),
        )
        .withColumn(
            "transaction_at_local",
            F.when(
                F.col("transaction_at").isNotNull(),
                F.from_utc_timestamp(F.col("transaction_at"), F.col("timezone_transaction")),
            ),
        )
    )

    # --- String Adjustment: INITCAP everywhere except club/location names ---
    string_cols_to_cap = [
        "referral_source",
        "referral_source_category",
        "referral_status",
        "referee_name",
        "referrer_name",
        "transaction_status",
        "transaction_type",
    ]
    club_name_columns = ["referrer_homeclub", "transaction_location"]
    df = initcap_except(df, string_cols_to_cap, exclude=club_name_columns)

    # --- Handling Nulls: replace missing descriptive values with explicit
    # placeholders instead of leaving blank cells (numeric/boolean/date
    # fields that describe an event which never happened are left NULL,
    # since inventing a fake transaction date would corrupt the fraud
    # logic below). ---
    df = df.fillna(
        {
            "referee_name": "Unknown",
            "referrer_name": "Unknown",
            "referrer_homeclub": "Unknown",
            "referrer_phone_number": "Unknown",
            "transaction_status": "No Transaction",
            "transaction_type": "No Transaction",
            "transaction_location": "Unknown",
            "referral_status": "Unknown",
            "referral_source_category": "Unknown",
            "num_reward_days": 0,
        }
    ).fillna(False, subset=["is_reward_granted"])

    return df


# --------------------------------------------------------------------------- #
# 4. Business Logic: fraud / validity detection
# --------------------------------------------------------------------------- #
def apply_business_logic(df):
    reward_positive = (F.col("num_reward_days") > 0)
    no_reward = ~reward_positive
    status_berhasil = F.col("referral_status") == "Berhasil"
    status_pending_or_failed = F.col("referral_status").isin("Menunggu", "Tidak Berhasil")
    has_txn_id = F.col("transaction_id").isNotNull()
    txn_paid = F.col("transaction_status") == "Paid"
    txn_new = F.col("transaction_type") == "New"
    txn_after_referral = F.col("transaction_at") > F.col("referral_at")
    txn_before_referral = F.col("transaction_at") < F.col("referral_at")
    same_month = F.date_format(F.col("transaction_at"), "yyyy-MM") == F.date_format(
        F.col("referral_at"), "yyyy-MM"
    )
    membership_not_expired = F.col("referrer_membership_expired_date") >= F.to_date(F.col("transaction_at"))
    referrer_not_deleted = F.col("referrer_is_deleted") == False  # noqa: E712
    reward_granted = F.col("is_reward_granted") == True  # noqa: E712

    # ---- VALID conditions ----
    valid_condition_1 = (
        reward_positive
        & status_berhasil
        & has_txn_id
        & txn_paid
        & txn_new
        & txn_after_referral
        & same_month
        & membership_not_expired
        & referrer_not_deleted
        & reward_granted
    )
    valid_condition_2 = status_pending_or_failed & no_reward

    # ---- INVALID conditions (kept as their own flags -> nice-to-have
    # `fraud_reason` explanation column) ----
    invalid_condition_1 = reward_positive & ~status_berhasil
    invalid_condition_2 = reward_positive & ~has_txn_id
    invalid_condition_3 = no_reward & has_txn_id & txn_paid & txn_after_referral
    invalid_condition_4 = status_berhasil & no_reward
    invalid_condition_5 = F.col("transaction_at").isNotNull() & txn_before_referral

    df = df.withColumn(
        "is_business_logic_valid",
        F.when(valid_condition_1 | valid_condition_2, F.lit(True)).otherwise(F.lit(False)),
    )

    df = df.withColumn(
        "fraud_reason",
        F.when(valid_condition_1 | valid_condition_2, F.lit(None))
        .when(invalid_condition_5, F.lit("Transaction occurred before the referral was created"))
        .when(invalid_condition_2, F.lit("Reward value assigned but referral has no transaction ID"))
        .when(invalid_condition_1, F.lit("Reward value assigned but referral status is not Berhasil"))
        .when(invalid_condition_4, F.lit("Referral status is Berhasil but reward value is null/zero"))
        .when(invalid_condition_3, F.lit("No reward assigned but a paid transaction exists after referral"))
        .otherwise(F.lit("Does not satisfy any known valid pattern")),
    )
    return df


# --------------------------------------------------------------------------- #
# 5. Output
# --------------------------------------------------------------------------- #
def write_report(df, output_dir: str):
    w = Window.orderBy("referral_id")
    df = df.withColumn("referral_details_id", F.row_number().over(w))

    final_df = df.select(
        "referral_details_id",
        "referral_id",
        "referral_source",
        "referral_source_category",
        F.col("referral_at_local").alias("referral_at"),
        "referrer_id",
        "referrer_name",
        "referrer_phone_number",
        "referrer_homeclub",
        "referee_id",
        "referee_name",
        "referee_phone",
        "referral_status",
        "num_reward_days",
        "transaction_id",
        "transaction_status",
        F.col("transaction_at_local").alias("transaction_at"),
        "transaction_location",
        "transaction_type",
        F.col("updated_at_local").alias("updated_at"),
        F.col("reward_granted_at_local").alias("reward_granted_at"),
        "is_business_logic_valid",
        "fraud_reason",
    ).orderBy("referral_details_id")

    os.makedirs(output_dir, exist_ok=True)
    pdf = final_df.toPandas()

    csv_path = os.path.join(output_dir, "referral_fraud_report.csv")
    pdf.to_csv(csv_path, index=False)
    print(f"[INFO] Report written: {csv_path} ({len(pdf)} rows)")
    return pdf


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(input_dir: str, output_dir: str):
    spark = SparkSession.builder.appName("ReferralFraudPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[INFO] Loading data...")
    tables = load_data(spark, input_dir)

    print("[INFO] Cleaning data...")
    tables = clean_data(tables)

    print("[INFO] Processing / joining data...")
    df = process_data(tables)

    print("[INFO] Applying fraud-detection business logic...")
    df = apply_business_logic(df)

    print("[INFO] Writing final report...")
    write_report(df, output_dir)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Referral program fraud-detection pipeline.")
    parser.add_argument("--input-dir", default="data/raw", help="Directory containing raw CSV files")
    parser.add_argument("--output-dir", default="output/report", help="Directory to write the final report")
    args = parser.parse_args()
    main(args.input_dir, args.output_dir)
