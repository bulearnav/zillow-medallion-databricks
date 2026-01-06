# 02_gold_aggregations_standalone.py
#
# Creates Gold layer aggregation tables using standard PySpark (non-DLT).
# This is an alternative to the DLT pipeline for environments without DLT access.
#
# Tables Created:
# - dim_date - Date dimension
# - dim_property_type - Property type dimension
# - fact_housing_metrics - Core fact table
# - agg_monthly_regional - Monthly aggregations
# - agg_top_regions_by_value - Top 10 by home value
# - agg_yearly_trend - Yearly trends

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

CAT = "zillow"
# Updated to match DLT pipeline output schema
SILVER = "zillow_silver_dlt"
GOLD = "zillow_gold"

run_id = str(uuid.uuid4())

# Create Gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{GOLD}")

print(f"Run ID: {run_id}")
print(f"Silver Source: {CAT}.{SILVER}")
print(f"Gold Target: {CAT}.{GOLD}")

# ==================== DIM_DATE ====================
print("Creating dim_date...")

# Get distinct dates from Silver
df_dates = spark.table(f"{CAT}.{SILVER}.zip_ts_silver").select("date").distinct()

dim_date = (df_dates
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.col("quarter")))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("is_month_end", F.col("date") == F.last_day("date"))
    .select("date_key", "date", "year", "quarter", "month", "month_name", 
            "year_month", "year_quarter", "day_of_week", "is_month_end")
)

dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{GOLD}.dim_date")
spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.dim_date IS 'Date dimension with calendar hierarchy'")
print(f"  Created with {dim_date.count():,} rows")

# ==================== DIM_PROPERTY_TYPE ====================
print("\nCreating dim_property_type...")

property_types = [
    (1, "all_homes", "All Homes", "All residential property types combined"),
    (2, "single_family_residence", "Single Family", "Detached single-family homes"),
    (3, "condo_coop", "Condo/Co-op", "Condominiums and cooperative units"),
    (4, "bottom_tier", "Bottom Tier", "Bottom third of home values in a metro"),
    (5, "middle_tier", "Middle Tier", "Middle third of home values in a metro"),
    (6, "top_tier", "Top Tier", "Top third of home values in a metro"),
    (7, "1_bedroom", "1 Bedroom", "One bedroom properties"),
    (8, "2_bedroom", "2 Bedroom", "Two bedroom properties"),
    (9, "3_bedroom", "3 Bedroom", "Three bedroom properties"),
    (10, "4_bedroom", "4 Bedroom", "Four bedroom properties"),
    (11, "5_bedroom_or_more", "5+ Bedroom", "Five or more bedroom properties"),
]

dim_property = spark.createDataFrame(property_types,
    ["property_type_key", "property_type_code", "property_type_name", "description"]
)

dim_property.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{GOLD}.dim_property_type")
spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.dim_property_type IS 'Property type dimension with Zillow categories'")
print(f"  Created with {dim_property.count()} rows")

# ==================== FACT_HOUSING_METRICS ====================
print("\nCreating fact_housing_metrics...")

# Core columns to extract
core_cols = [
    "date", "region_name",
    "zhvi_all_homes", "zhvi_single_family_residence", "zhvi_condo_coop",
    "zhvi_bottom_tier", "zhvi_middle_tier", "zhvi_top_tier",
    "zri_all_homes",
    "median_listing_price_all_homes",
    "median_listing_price_per_sqft_all_homes",
    "inventory_raw_all_homes",
    "price_to_rent_ratio_all_homes",
    "pct_of_homes_increasing_in_values_all_homes",
    "pct_of_homes_decreasing_in_values_all_homes"
]

tables = [
    (f"{CAT}.{SILVER}.city_ts_silver", "city"),
    (f"{CAT}.{SILVER}.metro_ts_silver", "metro"),
    (f"{CAT}.{SILVER}.zip_ts_silver", "zip"),
    (f"{CAT}.{SILVER}.county_ts_silver", "county"),
]

dfs = []
for table_name, level in tables:
    try:
        df = spark.table(table_name)
        available_cols = [c for c in core_cols if c in df.columns]
        df = df.select(*available_cols).withColumn("region_level", F.lit(level))
        dfs.append(df)
        print(f"  Added {level}: {df.count():,} rows")
    except Exception as e:
        print(f"  Skipped {level}: {e}")

if dfs:
    from functools import reduce
    fact = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    
    fact = (fact
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("region_key", F.md5(F.concat_ws("|", "region_name", "region_level")))
        .withColumn("fact_key", F.md5(F.concat_ws("|", "date", "region_name", "region_level")))
    )
    
    (fact.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .saveAsTable(f"{CAT}.{GOLD}.fact_housing_metrics")
    )
    
    spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.fact_housing_metrics IS 'Fact table: Housing metrics by region and date, partitioned by year/month'")
    print(f"\n  Total fact rows: {fact.count():,}")

# ==================== AGG_MONTHLY_REGIONAL ====================
print("\nCreating agg_monthly_regional...")

fact = spark.table(f"{CAT}.{GOLD}.fact_housing_metrics")

agg_monthly = (fact
    .groupBy("year", "month", "region_level")
    .agg(
        F.count("*").alias("record_count"),
        F.countDistinct("region_name").alias("region_count"),
        F.avg("zhvi_all_homes").alias("avg_zhvi"),
        F.min("zhvi_all_homes").alias("min_zhvi"),
        F.max("zhvi_all_homes").alias("max_zhvi"),
        F.percentile_approx("zhvi_all_homes", 0.5).alias("median_zhvi"),
        F.avg("zri_all_homes").alias("avg_zri"),
        F.avg("median_listing_price_all_homes").alias("avg_listing_price"),
        F.sum("inventory_raw_all_homes").alias("total_inventory"),
        F.avg("price_to_rent_ratio_all_homes").alias("avg_price_to_rent")
    )
    .withColumn("year_month", F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0")))
    .orderBy("year", "month", "region_level")
)

agg_monthly.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{GOLD}.agg_monthly_regional")
spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.agg_monthly_regional IS 'Monthly aggregated housing metrics by region level'")
print(f"  Created with {agg_monthly.count():,} rows")

# ==================== AGG_TOP_REGIONS_BY_VALUE ====================
print("\nCreating agg_top_regions_by_value...")

latest_date = fact.agg(F.max("date")).collect()[0][0]
print(f"  Latest date: {latest_date}")

agg_top = (fact
    .filter(F.col("date") == latest_date)
    .filter(F.col("region_level") == "city")
    .filter(F.col("zhvi_all_homes").isNotNull())
    .orderBy(F.desc("zhvi_all_homes"))
    .limit(10)
    .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("zhvi_all_homes"))))
    .select("rank", "region_name", "region_level", "zhvi_all_homes", 
            "zri_all_homes", "median_listing_price_all_homes", "date")
)

agg_top.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{GOLD}.agg_top_regions_by_value")
spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.agg_top_regions_by_value IS 'Top 10 cities by home value (ZHVI) - latest snapshot'")
print(f"  Created with {agg_top.count()} rows")

# ==================== AGG_YEARLY_TREND ====================
print("\nCreating agg_yearly_trend...")

yearly = (fact
    .groupBy("year", "region_level")
    .agg(
        F.count("*").alias("total_observations"),
        F.countDistinct("region_name").alias("region_count"),
        F.avg("zhvi_all_homes").alias("avg_zhvi"),
        F.avg("zri_all_homes").alias("avg_zri"),
        F.avg("median_listing_price_all_homes").alias("avg_listing_price"),
        F.avg("price_to_rent_ratio_all_homes").alias("avg_price_to_rent")
    )
)

window = Window.partitionBy("region_level").orderBy("year")
agg_yearly = (yearly
    .withColumn("prev_avg_zhvi", F.lag("avg_zhvi").over(window))
    .withColumn("yoy_zhvi_change_pct",
        F.when(F.col("prev_avg_zhvi").isNotNull(),
            F.round(((F.col("avg_zhvi") - F.col("prev_avg_zhvi")) / F.col("prev_avg_zhvi")) * 100, 2)
        )
    )
    .drop("prev_avg_zhvi")
    .orderBy("year", "region_level")
)

agg_yearly.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{GOLD}.agg_yearly_trend")
spark.sql(f"COMMENT ON TABLE {CAT}.{GOLD}.agg_yearly_trend IS 'Yearly trend with YoY growth by region level'")
print(f"  Created with {agg_yearly.count():,} rows")

# ==================== SUMMARY ====================
print("\n" + "="*60)
print("GOLD LAYER TABLES CREATED")
print("="*60)

display(spark.sql(f"SHOW TABLES IN {CAT}.{GOLD}"))
