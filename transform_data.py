import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text  # Import text function
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL Connection
DATABASE_URL = os.getenv("DATABASE_URL")

# Retry connecting to the database
MAX_RETRIES = 3
for attempt in range(MAX_RETRIES):
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as connection:
            print("✅ Connected to PostgreSQL database!")
        break  # Connection successful
    except OperationalError as e:
        print(f"⚠️ Database connection failed (Attempt {attempt + 1}/{MAX_RETRIES})")
        print(f"Error: {e}")
        time.sleep(5)  # Wait before retrying
else:
    print("❌ Failed to connect after multiple attempts. Exiting.")
    exit(1)

# Check if required tables exist
check_tables_query = text("""
    SELECT tablename FROM pg_tables WHERE schemaname = 'public';
""")

with engine.connect() as connection:
    result = connection.execute(check_tables_query)
    existing_tables = [row[0] for row in result.fetchall()]  # Fetch results correctly

print(f"Existing Tables: {existing_tables}")

if "sales_data" not in existing_tables or "production_data" not in existing_tables:
    print("❌ Tables do not exist! Run ingest_data.py first.")
    exit(1)

# Load Data from PostgreSQL
sales_query = "SELECT date, product, region, units_sold, price_per_unit FROM sales_data;"
sales_df = pd.read_sql(sales_query, engine)

production_query = "SELECT date, product, units_produced, spoiled_units, cost_per_unit FROM production_data;"
production_df = pd.read_sql(production_query, engine)

# Debug: Check Initial DataFrames
print("Checking sales_data before processing:")
print(sales_df.head())

print("Checking production_data before processing:")
print(production_df.head())

# Ensure cost_per_unit is numeric (remove "$" if present)
production_df["cost_per_unit"] = pd.to_numeric(
    production_df["cost_per_unit"].astype(str).str.replace("$", ""), errors="coerce"
)

# Compute revenue
sales_df["revenue"] = sales_df["units_sold"] * sales_df["price_per_unit"]

# Create Sales Summary
# Convert `price_per_unit` to numeric (remove "$" if present)
sales_df["price_per_unit"] = pd.to_numeric(sales_df["price_per_unit"].astype(str).str.replace("$", ""), errors="coerce")

# Compute revenue
sales_df["revenue"] = sales_df["units_sold"] * sales_df["price_per_unit"]

# Aggregate Sales Summary
sales_summary = sales_df.groupby(["product", "region"]).agg(
    total_units_sold=("units_sold", "sum"),
    total_revenue=("revenue", "sum")  # This now correctly sums revenue
).reset_index()

# Debug: Check Summarized Data
print("Checking Updated Sales Summary:")
print(sales_summary.head())

# Compute efficiency for production
production_df["efficiency"] = (production_df["units_produced"] - production_df["spoiled_units"]) / production_df["units_produced"]

# Create Production Summary
production_summary = production_df.groupby("product").agg(
    total_units_produced=("units_produced", "sum"),
    total_spoiled=("spoiled_units", "sum"),
    avg_efficiency=("efficiency", "mean"),
    avg_cost_per_unit=("cost_per_unit", "mean")  # Ensure avg cost is computed
).reset_index()

# Debug: Check Summarized Data
print("Sales Summary:")
print(sales_summary.head())

print("Production Summary:")
print(production_summary.head())

# Ensure numeric conversions for revenue and units produced
sales_summary["total_revenue"] = pd.to_numeric(sales_summary["total_revenue"], errors="coerce")
production_summary["total_units_produced"] = pd.to_numeric(production_summary["total_units_produced"], errors="coerce")

# Merge Sales and Production Summaries
profitability_df = sales_summary.merge(production_summary, on="product", how="left")

# Debug: Check `profitability_df` after merge
print("Checking profitability_df after merge:")
print(profitability_df.head())

# Handle Missing Values (Fill NaN before Computation)
profitability_df.fillna({"total_revenue": 0, "total_units_produced": 0, "avg_cost_per_unit": production_df["cost_per_unit"].mean()}, inplace=True)

# Compute Profitability
profitability_df["total_cost"] = profitability_df["total_units_produced"] * profitability_df["avg_cost_per_unit"]
profitability_df["profit"] = profitability_df["total_revenue"] - profitability_df["total_cost"]

# Debug: Check Final Computed Values
print("Checking Computed Revenue & Profit:")
print(profitability_df[["product", "total_revenue", "total_cost", "profit"]].head())

# Store Transformed Data in PostgreSQL
sales_summary.to_sql("sales_summary", engine, if_exists="replace", index=False)
production_summary.to_sql("production_summary", engine, if_exists="replace", index=False)
profitability_df.to_sql("profitability_summary", engine, if_exists="replace", index=False)

print("✅ Data transformation complete.")