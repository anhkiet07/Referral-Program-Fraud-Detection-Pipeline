"""
utils.py
--------
Small helpers shared between profiling.py and referral_pipeline.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The raw CSV exports contain the literal text "null" (and blank strings)
# for missing values instead of a real NULL. Both must be normalized to a
# real NULL before any cleaning/business logic can rely on isNull() checks.
NULL_TOKENS = ["null", "NULL", "Null", "", "NaN", "nan"]


def normalize_nulls(df: DataFrame) -> DataFrame:
    """Replace textual null markers with real NULLs, for every string column."""
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)).isin(NULL_TOKENS), None).otherwise(F.col(field.name)),
            )
    return df


def initcap_except(df: DataFrame, columns: list, exclude: list = None) -> DataFrame:
    """Apply INITCAP() to the given string columns, skipping any column
    named in `exclude` (per spec: club/location names must stay as-is)."""
    exclude = exclude or []
    for col_name in columns:
        if col_name in exclude:
            continue
        df = df.withColumn(col_name, F.initcap(F.col(col_name)))
    return df
