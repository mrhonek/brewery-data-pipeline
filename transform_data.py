import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Use Railway PostgreSQL URL with SSL enabled
DATABASE_URL = "postgresql://postgres:***REMOVED***@postgres.railway.internal:5432/railway?sslmode=require"

# Retry connecting to the database
MAX_RETRIES = 3
for attempt in range(MAX_RETRIES):
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as connection:
            print("✅ Connected to PostgreSQL on Railway!")
        break  # Connection successful
    except OperationalError as e:
        print(f"⚠️ Database connection failed (Attempt {attempt + 1}/{MAX_RETRIES})")
        print(f"Error: {e}")
        time.sleep(5)  # Wait before retrying
else:
    print("❌ Failed to connect after multiple attempts. Exiting.")
    exit(1)

from sqlalchemy.sql import text  # Import text function from sqlalchemy.sql

# Check if tables exist before querying
check_tables_query = """
    SELECT tablename FROM pg_tables WHERE schemaname = 'public';
"""

with engine.connect() as connection:
    existing_tables = [row[0] for row in connection.execute(check_tables_query)]
    print(f"📊 Existing Tables: {existing_tables}")

if "sales_data" not in existing_tables or "production_data" not in existing_tables:
    print("❌ Tables do not exist! Run ingest_data.py first.")
    exit(1)

# Continue with data transformation
sales_query = "SELECT date, product, region, units_sold, price_per_unit FROM sales_data;"
sales_df = pd.read_sql(sales_query, engine)

production_query = "SELECT date, product, units_produced, spoiled_units, cost_per_unit FROM production_data;"
production_df = pd.read_sql(production_query, engine)

# Perform data aggregation
sales_df["revenue"] = sales_df["units_sold"] * sales_df["price_per_unit"]
sales_summary = sales_df.groupby(["product", "region"]).agg(
    total_units_sold=("units_sold", "sum"),
    total_revenue=("revenue", "sum")
).reset_index()

production_df["efficiency"] = (production_df["units_produced"] - production_df["spoiled_units"]) / production_df["units_produced"]
production_summary = production_df.groupby("product").agg(
    total_units_produced=("units_produced", "sum"),
    total_spoiled=("spoiled_units", "sum"),
    avg_efficiency=("efficiency", "mean")
).reset_index()

profitability_df = sales_summary.merge(production_summary, on="product", how="left")
profitability_df["total_cost"] = profitability_df["total_units_produced"] * production_df["cost_per_unit"].mean()
profitability_df["profit"] = profitability_df["total_revenue"] - profitability_df["total_cost"]

# Store transformed data in PostgreSQL
sales_summary.to_sql("sales_summary", engine, if_exists="replace", index=False)
production_summary.to_sql("production_summary", engine, if_exists="replace", index=False)
profitability_df.to_sql("profitability_summary", engine, if_exists="replace", index=False)

print("✅ Data transformation complete.")