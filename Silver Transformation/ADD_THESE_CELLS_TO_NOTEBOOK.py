# ============================================================
# ADD THESE TWO CELLS TO: 04_dlt_silver_pipeline.ipynb
# 
# Open the notebook in Databricks and add these cells at the end
# (after the county_crosswalk_silver cell)
# ============================================================


# ==================== CELL 1: ZIP TIME SERIES ====================
# Copy this entire block into a new cell

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


# ==================== CELL 2: COUNTY TIME SERIES ====================
# Copy this entire block into a new cell

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
