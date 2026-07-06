"""
profiling.py
------------
Profiles every raw source table and writes one CSV report per table to
`output/profiling/`. For every column we compute:

    - data_type            (Spark-inferred type)
    - row_count             total rows in the table
    - null_count            number of NULL / "null" / empty values
    - null_percentage       null_count / row_count
    - distinct_count        count of distinct non-null values
    - min_value / max_value (for numeric & date/timestamp columns)
    - sample_values         a few example values (helps business users)

This satisfies Test Instruction #1 (Data Profiling): "at least find the
null count and distinct value count" for every column of every table.

Usage:
    python src/profiling.py --input-dir data/raw --output-dir output/profiling
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, DateType, TimestampType

# The literal string "null" shows up in the raw CSVs (e.g. referee_id=null)
# in addition to genuinely empty/NULL cells, so both must be treated as missing.
NULL_TOKENS = ["null", "NULL", "Null", "", "NaN", "nan"]

SOURCE_TABLES = [
    "lead_log",
    "paid_transactions",
    "referral_rewards",
    "user_logs",
    "user_referral_logs",
    "user_referral_statuses",
    "user_referrals",
]


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV keeping every column as string so profiling never fails
    on mixed / dirty data, and normalize the textual "null" token to a
    real NULL so null-counting is accurate."""
    df = spark.read.csv(path, header=True, inferSchema=True, mode="PERMISSIVE")
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name).cast("string")).isin(NULL_TOKENS), None)
            .otherwise(F.col(col_name)),
        )
    return df


def profile_table(spark: SparkSession, table_name: str, df: DataFrame) -> DataFrame:
    total_rows = df.count()
    rows = []

    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType

        null_count = df.filter(F.col(col_name).isNull()).count()
        distinct_count = df.select(col_name).filter(F.col(col_name).isNotNull()).distinct().count()

        min_value = max_value = None
        if isinstance(dtype, (NumericType, DateType, TimestampType)):
            agg = df.agg(F.min(col_name).alias("mn"), F.max(col_name).alias("mx")).collect()[0]
            min_value, max_value = agg["mn"], agg["mx"]

        sample_values = (
            df.select(col_name)
            .filter(F.col(col_name).isNotNull())
            .distinct()
            .limit(3)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        rows.append(
            {
                "table_name": table_name,
                "column_name": col_name,
                "data_type": str(dtype.simpleString()),
                "row_count": total_rows,
                "null_count": null_count,
                "null_percentage": round((null_count / total_rows) * 100, 2) if total_rows else 0.0,
                "distinct_value_count": distinct_count,
                "min_value": str(min_value) if min_value is not None else "",
                "max_value": str(max_value) if max_value is not None else "",
                "sample_values": " | ".join(str(v) for v in sample_values),
            }
        )

    return spark.createDataFrame(rows)


def main(input_dir: str, output_dir: str) -> None:
    spark = SparkSession.builder.appName("ReferralDataProfiling").getOrCreate()
    os.makedirs(output_dir, exist_ok=True)

    all_profiles = []
    for table in SOURCE_TABLES:
        csv_path = os.path.join(input_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            print(f"[WARN] Skipping missing file: {csv_path}")
            continue

        print(f"[INFO] Profiling table: {table}")
        df = read_csv(spark, csv_path)
        profile_df = profile_table(spark, table, df)
        all_profiles.append(profile_df)

        # One CSV per table for easy business-user consumption
        table_out = os.path.join(output_dir, f"profile_{table}")
        profile_df.orderBy("column_name").toPandas().to_csv(f"{table_out}.csv", index=False)

    # Combined profiling report across all tables
    if all_profiles:
        combined = all_profiles[0]
        for extra in all_profiles[1:]:
            combined = combined.unionByName(extra)
        combined.orderBy("table_name", "column_name").toPandas().to_csv(
            os.path.join(output_dir, "profile_all_tables.csv"), index=False
        )

    print(f"[INFO] Profiling complete. Reports written to: {output_dir}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profile all referral-program source tables.")
    parser.add_argument("--input-dir", default="data/raw", help="Directory containing raw CSV files")
    parser.add_argument("--output-dir", default="output/profiling", help="Directory to write profiling CSVs")
    args = parser.parse_args()
    main(args.input_dir, args.output_dir)
