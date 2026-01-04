# Databricks notebook source
# MAGIC %md
# MAGIC # UDF Library for Zillow Silver Layer
# MAGIC 
# MAGIC This module contains User-Defined Functions (UDFs) for:
# MAGIC - Column name standardization
# MAGIC - Date parsing
# MAGIC - Data validation
# MAGIC - Record hashing

# COMMAND ----------

import re
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, BooleanType
from pyspark.sql import DataFrame
from typing import List

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Column Name Standardization UDF

# COMMAND ----------

def standardize_column_name(col_name: str) -> str:
    """
    Convert column name to snake_case.
    
    Examples:
        MedianListingPrice_AllHomes -> median_listing_price_all_homes
        ZHVIPerSqft_AllHomes -> zhvi_per_sqft_all_homes
        Date -> date
    
    Args:
        col_name: Original column name
        
    Returns:
        Standardized snake_case column name
    """
    # Insert underscore before uppercase letters (except at start)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    # Insert underscore before uppercase letters that follow lowercase
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    # Replace multiple underscores with single underscore
    s3 = re.sub('_+', '_', s2)
    # Convert to lowercase and strip leading/trailing underscores
    return s3.lower().strip('_')

# Register as UDF for use in SQL
standardize_column_name_udf = F.udf(standardize_column_name, StringType())

# COMMAND ----------

def standardize_dataframe_columns(df: DataFrame) -> DataFrame:
    """
    Standardize all column names in a DataFrame to snake_case.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    new_columns = [standardize_column_name(col) for col in df.columns]
    return df.toDF(*new_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Date Parsing UDF

# COMMAND ----------

def parse_zillow_date(date_str: str) -> str:
    """
    Parse Zillow date format (YYYY-MM-DD or MM/DD/YYYY) to standard format.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        Standardized date string (YYYY-MM-DD)
    """
    if date_str is None:
        return None
    
    date_str = str(date_str).strip()
    
    # Already in correct format
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    # MM/DD/YYYY format
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
    if match:
        month, day, year = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    
    return date_str

parse_zillow_date_udf = F.udf(parse_zillow_date, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Numeric Validation UDF

# COMMAND ----------

def validate_numeric_range(value, min_val: float = None, max_val: float = None) -> bool:
    """
    Validate if a numeric value is within expected range.
    
    Args:
        value: Numeric value to validate
        min_val: Minimum allowed value (optional)
        max_val: Maximum allowed value (optional)
        
    Returns:
        True if valid, False otherwise
    """
    if value is None:
        return True  # Nulls handled separately
    
    try:
        num = float(value)
        if min_val is not None and num < min_val:
            return False
        if max_val is not None and num > max_val:
            return False
        return True
    except (ValueError, TypeError):
        return False

validate_numeric_range_udf = F.udf(validate_numeric_range, BooleanType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Record Hash UDF

# COMMAND ----------

def generate_record_hash(*args) -> str:
    """
    Generate MD5 hash from record values for deduplication.
    
    Args:
        *args: Column values to hash
        
    Returns:
        MD5 hash string
    """
    combined = '|'.join(str(arg) if arg is not None else '' for arg in args)
    return hashlib.md5(combined.encode()).hexdigest()

# Note: For Spark, we'll use built-in md5() function instead
# This is provided for reference/Pandas UDF usage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Validation Functions

# COMMAND ----------

def get_quality_rules() -> dict:
    """
    Returns data quality rules for Zillow dataset.
    """
    return {
        # Required fields (should not be null)
        "required_fields": ["date", "region_name"],
        
        # Numeric ranges (field: (min, max))
        "numeric_ranges": {
            "zhvi_all_homes": (0, 10000000),  # Home values $0-$10M
            "zri_all_homes": (0, 50000),       # Rent $0-$50K/month
            "median_listing_price_all_homes": (0, 50000000),
            "price_to_rent_ratio_all_homes": (0, 100),
        },
        
        # Percentage fields (should be 0-100 or 0-1)
        "percentage_fields": [
            "pct_of_homes_decreasing_in_values_all_homes",
            "pct_of_homes_increasing_in_values_all_homes",
            "pct_of_listings_with_price_reductions_all_homes",
        ]
    }

# COMMAND ----------

def validate_record(row: dict, rules: dict = None) -> tuple:
    """
    Validate a record against quality rules.
    
    Args:
        row: Dictionary of column values
        rules: Quality rules dictionary
        
    Returns:
        Tuple of (is_valid: bool, failure_reasons: List[str])
    """
    if rules is None:
        rules = get_quality_rules()
    
    reasons = []
    
    # Check required fields
    for field in rules.get("required_fields", []):
        if field in row and (row[field] is None or str(row[field]).strip() == ''):
            reasons.append(f"Missing required field: {field}")
    
    # Check numeric ranges
    for field, (min_val, max_val) in rules.get("numeric_ranges", {}).items():
        if field in row and row[field] is not None:
            try:
                val = float(row[field])
                if val < min_val or val > max_val:
                    reasons.append(f"Out of range: {field}={val} (expected {min_val}-{max_val})")
            except (ValueError, TypeError):
                reasons.append(f"Invalid numeric: {field}={row[field]}")
    
    return (len(reasons) == 0, reasons)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Utility Functions

# COMMAND ----------

def get_common_columns() -> List[str]:
    """
    Returns list of columns common across all time series tables.
    These are the core metrics to transform.
    """
    return [
        "date",
        "region_name",
        "zhvi_all_homes",
        "zhvi_single_family_residence",
        "zhvi_condo_coop",
        "zhvi_bottom_tier",
        "zhvi_middle_tier",
        "zhvi_top_tier",
        "zri_all_homes",
        "zri_all_homes_plus_multifamily",
        "median_listing_price_all_homes",
        "median_listing_price_per_sqft_all_homes",
        "inventory_raw_all_homes",
        "inventory_seasonally_adjusted_all_homes",
        "price_to_rent_ratio_all_homes",
        "pct_of_homes_increasing_in_values_all_homes",
        "pct_of_homes_decreasing_in_values_all_homes",
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC 
# MAGIC ### UDF Summary
# MAGIC 
# MAGIC | Function | Type | Purpose |
# MAGIC |----------|------|---------|
# MAGIC | `standardize_column_name` | Python/UDF | Convert CamelCase to snake_case |
# MAGIC | `standardize_dataframe_columns` | Python | Apply to entire DataFrame |
# MAGIC | `parse_zillow_date` | Python/UDF | Parse date strings to standard format |
# MAGIC | `validate_numeric_range` | Python/UDF | Check value within min/max bounds |
# MAGIC | `generate_record_hash` | Python | Create MD5 hash for deduplication |
# MAGIC | `get_quality_rules` | Python | Return validation rule definitions |
# MAGIC | `validate_record` | Python | Validate single record against rules |
# MAGIC 
# MAGIC ### Usage Example
# MAGIC 
# MAGIC ```python
# MAGIC # In another notebook:
# MAGIC %run "./common/udf_library"
# MAGIC 
# MAGIC # Standardize columns
# MAGIC df_clean = standardize_dataframe_columns(df_bronze)
# MAGIC 
# MAGIC # Parse dates
# MAGIC df_clean = df_clean.withColumn("date", F.to_date(parse_zillow_date_udf(F.col("date"))))
# MAGIC ```
