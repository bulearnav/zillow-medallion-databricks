# 04_dlt_silver_pipeline.py
# 
# Delta Live Tables pipeline for Silver layer with:
# - Streaming from Bronze tables
# - Data quality expectations (constraints)
#
# Note: This is designed to run as a DLT pipeline, not standalone.
# Deploy via the `resources/pipelines.yml` configuration.

import dlt
from pyspark.sql import functions as F
import re


def standardize_column_name(col_name: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = re.sub('_+', '_', s2)
    return s3.lower().strip('_')


def standardize_columns(df):
    """Apply snake_case to all columns."""
    for col in df.columns:
        new_name = standardize_column_name(col)
        if new_name != col:
            df = df.withColumnRenamed(col, new_name)
    return df


# ==================== CITY TIME SERIES ====================
@dlt.table(
    name="city_ts_silver",
    comment="Silver: City time series with cleaned and standardized data"
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region_name IS NOT NULL AND region_name != ''")
@dlt.expect("zhvi_positive", "zhvi_all_homes IS NULL OR zhvi_all_homes >= 0")
def city_ts_silver():
    # Read from Bronze
    df = spark.table("zillow.zillow_bronze.city_ts_bronze")
    
    # Drop bronze audit columns
    for col in ['load_dt', 'source_path', 'ingest_mode']:
        if col in df.columns:
            df = df.drop(col)
    
    # Standardize column names
    df = standardize_columns(df)
    
    # Clean and transform
    df = (df
        .withColumn("date", F.to_date(F.col("date").cast("string")))
        .withColumn("processed_dt", F.current_timestamp())
        .withColumn("source_table", F.lit("city_ts_bronze"))
    )
    
    # Deduplicate
    df = df.dropDuplicates(["date", "region_name"])
    
    return df


# ==================== METRO TIME SERIES ====================
@dlt.table(
    name="metro_ts_silver",
    comment="Silver: Metro time series with cleaned and standardized data"
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region_name IS NOT NULL AND region_name != ''")
@dlt.expect("zhvi_positive", "zhvi_all_homes IS NULL OR zhvi_all_homes >= 0")
def metro_ts_silver():
    df = spark.table("zillow.zillow_bronze.metro_ts_bronze")
    
    for col in ['load_dt', 'source_path', 'ingest_mode']:
        if col in df.columns:
            df = df.drop(col)
    
    df = standardize_columns(df)
    
    df = (df
        .withColumn("date", F.to_date(F.col("date").cast("string")))
        .withColumn("processed_dt", F.current_timestamp())
        .withColumn("source_table", F.lit("metro_ts_bronze"))
    )
    
    df = df.dropDuplicates(["date", "region_name"])
    
    return df


# ==================== ZIP TIME SERIES ====================
@dlt.table(
    name="zip_ts_silver",
    comment="Silver: Zip code time series with cleaned and standardized data"
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region_name IS NOT NULL AND region_name != ''")
@dlt.expect("zhvi_positive", "zhvi_all_homes IS NULL OR zhvi_all_homes >= 0")
def zip_ts_silver():
    df = spark.table("zillow.zillow_bronze.zip_ts_bronze")
    
    # Drop bronze audit columns (including part_id from incremental load)
    for col in ['load_dt', 'source_path', 'ingest_mode', 'part_id']:
        if col in df.columns:
            df = df.drop(col)
    
    df = standardize_columns(df)
    
    df = (df
        .withColumn("date", F.to_date(F.col("date").cast("string")))
        .withColumn("processed_dt", F.current_timestamp())
        .withColumn("source_table", F.lit("zip_ts_bronze"))
    )
    
    df = df.dropDuplicates(["date", "region_name"])
    
    return df


# ==================== COUNTY TIME SERIES ====================
@dlt.table(
    name="county_ts_silver",
    comment="Silver: County time series with cleaned and standardized data"
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region_name IS NOT NULL AND region_name != ''")
@dlt.expect("zhvi_positive", "zhvi_all_homes IS NULL OR zhvi_all_homes >= 0")
def county_ts_silver():
    df = spark.table("zillow.zillow_bronze.county_ts_json_bronze")
    
    for col in ['load_dt', 'source_path', 'ingest_mode']:
        if col in df.columns:
            df = df.drop(col)
    
    df = standardize_columns(df)
    
    df = (df
        .withColumn("date", F.to_date(F.col("date").cast("string")))
        .withColumn("processed_dt", F.current_timestamp())
        .withColumn("source_table", F.lit("county_ts_json_bronze"))
    )
    
    df = df.dropDuplicates(["date", "region_name"])
    
    return df


# ==================== CITIES CROSSWALK ====================
@dlt.table(
    name="cities_crosswalk_silver",
    comment="Silver: Cities crosswalk with standardized data"
)
@dlt.expect_or_drop("valid_city", "city IS NOT NULL AND city != ''")
def cities_crosswalk_silver():
    df = spark.table("zillow.zillow_bronze.cities_crosswalk_bronze")
    
    for col in ['load_dt', 'source_path', 'ingest_mode']:
        if col in df.columns:
            df = df.drop(col)
    
    df = standardize_columns(df)
    
    df = (df
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("processed_dt", F.current_timestamp())
    )
    
    df = df.dropDuplicates(["unique_city_id"])
    
    return df


# ==================== COUNTY CROSSWALK ====================
@dlt.table(
    name="county_crosswalk_silver",
    comment="Silver: County crosswalk with standardized FIPS codes"
)
@dlt.expect_or_drop("valid_county", "county_name IS NOT NULL AND county_name != ''")
@dlt.expect_or_drop("valid_fips", "fips IS NOT NULL")
def county_crosswalk_silver():
    df = spark.table("zillow.zillow_bronze.county_crosswalk_bronze")
    
    for col in ['load_dt', 'source_path', 'ingest_mode']:
        if col in df.columns:
            df = df.drop(col)
    
    df = standardize_columns(df)
    
    df = (df
        .withColumn("county_name", F.trim(F.col("county_name")))
        .withColumn("state_name", F.trim(F.col("state_name")))
        .withColumn("fips", F.lpad(F.col("fips").cast("string"), 5, "0"))
        .withColumn("processed_dt", F.current_timestamp())
    )
    
    df = df.dropDuplicates(["fips"])
    
    return df
