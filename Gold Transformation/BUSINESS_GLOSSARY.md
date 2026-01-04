# Zillow Housing Data - Business Glossary

## Overview
This document defines the business terms and metrics used in the Zillow Medallion data platform.

---

## Key Metrics

### Home Value Metrics

| Metric | Code | Description | Source |
|--------|------|-------------|--------|
| **Zillow Home Value Index (ZHVI)** | `zhvi_*` | Smoothed, seasonally adjusted measure of typical home value and market changes | Silver → Gold |
| **ZHVI All Homes** | `zhvi_all_homes` | ZHVI for all residential property types | Time Series |
| **ZHVI Single Family** | `zhvi_single_family_residence` | ZHVI for single-family detached homes | Time Series |
| **ZHVI Condo/Co-op** | `zhvi_condo_coop` | ZHVI for condos and co-ops | Time Series |
| **ZHVI Bottom Tier** | `zhvi_bottom_tier` | ZHVI for homes in bottom third of value in metro | Time Series |
| **ZHVI Middle Tier** | `zhvi_middle_tier` | ZHVI for homes in middle third of value in metro | Time Series |
| **ZHVI Top Tier** | `zhvi_top_tier` | ZHVI for homes in top third of value in metro | Time Series |

### Rental Metrics

| Metric | Code | Description | Source |
|--------|------|-------------|--------|
| **Zillow Rent Index (ZRI)** | `zri_*` | Smoothed measure of typical market rate rent | Silver → Gold |
| **ZRI All Homes** | `zri_all_homes` | ZRI for all rentable property types | Time Series |
| **Price-to-Rent Ratio** | `price_to_rent_ratio_all_homes` | Home value divided by annual rent (buy vs rent indicator) | Derived |

### Listing Metrics

| Metric | Code | Description | Source |
|--------|------|-------------|--------|
| **Median Listing Price** | `median_listing_price_all_homes` | Median price of homes listed for sale | Time Series |
| **Median Listing Price/SqFt** | `median_listing_price_per_sqft_all_homes` | Per square foot listing price | Time Series |
| **Inventory Raw** | `inventory_raw_all_homes` | Count of active listings | Time Series |
| **Inventory Seasonally Adjusted** | `inventory_seasonally_adjusted_all_homes` | Seasonally adjusted listing count | Time Series |

### Market Trend Metrics

| Metric | Code | Description | Source |
|--------|------|-------------|--------|
| **% Homes Increasing** | `pct_of_homes_increasing_in_values_all_homes` | Percentage of homes with increasing value | Time Series |
| **% Homes Decreasing** | `pct_of_homes_decreasing_in_values_all_homes` | Percentage of homes with decreasing value | Time Series |
| **YoY Growth %** | `yoy_zhvi_change_pct` | Year-over-year ZHVI change percentage | Gold Aggregation |

---

## Geographic Dimensions

| Level | Description | Example |
|-------|-------------|---------|
| **Zip** | 5-digit ZIP code | 10001 |
| **City** | City/town name | New York |
| **County** | County name | New York County |
| **Metro** | Metropolitan Statistical Area | New York-Newark-Jersey City |
| **State** | US State | New York |

---

## Property Type Categories

| Category | Code | Description |
|----------|------|-------------|
| All Homes | `all_homes` | Single-family, condo, co-op combined |
| Single Family | `single_family_residence` | Detached single-family homes |
| Condo/Co-op | `condo_coop` | Condominiums and cooperatives |
| Bottom Tier | `bottom_tier` | Lower third of values in metro |
| Middle Tier | `middle_tier` | Middle third of values in metro |
| Top Tier | `top_tier` | Upper third of values in metro |

---

## Data Quality Rules

| Rule | Field | Constraint | Action |
|------|-------|------------|--------|
| Required Date | `date` | NOT NULL | Drop record |
| Required Region | `region_name` | NOT NULL, non-empty | Drop record |
| Valid ZHVI | `zhvi_all_homes` | >= 0 | Quarantine |
| Valid ZRI | `zri_all_homes` | >= 0 | Quarantine |
| Valid Percentages | `pct_*` | 0-100 range | Flag for review |

---

## Gold Layer Tables

### Dimensions
| Table | Description | Key |
|-------|-------------|-----|
| `dim_date` | Calendar hierarchy | `date_key` |
| `dim_region` | Geographic hierarchy | `region_key` |
| `dim_property_type` | Property categories | `property_type_key` |

### Facts
| Table | Description | Grain |
|-------|-------------|-------|
| `fact_housing_metrics` | Core metrics | Date + Region |

### Aggregations
| Table | Description | Update Frequency |
|-------|-------------|-----------------|
| `agg_monthly_regional` | Monthly stats by region level | Monthly |
| `agg_top_regions_by_value` | Top 10 cities by ZHVI | Latest snapshot |
| `agg_top_regions_by_growth` | Top 10 by YoY growth | Latest snapshot |
| `agg_yearly_trend` | Annual trends by level | Yearly |
| `agg_market_health` | Market health scores | Latest snapshot |

---

## Acronyms

| Acronym | Definition |
|---------|------------|
| ZHVI | Zillow Home Value Index |
| ZRI | Zillow Rent Index |
| CBSA | Core Based Statistical Area |
| FIPS | Federal Information Processing Standards (geographic codes) |
| YoY | Year-over-Year |
| MoM | Month-over-Month |
