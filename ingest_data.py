import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text  # Import text() for raw SQL queries

# Use Railway PostgreSQL URL with SSL enabled
DATABASE_URL = "<YOUR_POSTGRES_URL>?sslmode=require"

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

# Create Tables (Fix using text())
with engine.connect() as connection:
    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            date DATE,
            product VARCHAR(50),
            region VARCHAR(50),
            units_sold INT,
            price_per_unit DECIMAL(5,2)
        );
    """))

    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS production_data (
            id SERIAL PRIMARY KEY,
            date DATE,
            product VARCHAR(50),
            units_produced INT,
            spoiled_units INT,
            cost_per_unit DECIMAL(5,2)
        );
    """))

print("✅ Tables created successfully.")

# Load CSV Data
sales_df = pd.read_csv("mock_sales_data.csv")
production_df = pd.read_csv("mock_production_data.csv")

# Insert Data into PostgreSQL
sales_df.to_sql("sales_data", engine, if_exists="append", index=False)
production_df.to_sql("production_data", engine, if_exists="append", index=False)

print("✅ Data successfully loaded into PostgreSQL.")

# Verify Row Count
with engine.connect() as connection:
    sales_count = connection.execute(text("SELECT COUNT(*) FROM sales_data;")).fetchone()[0]
    production_count = connection.execute(text("SELECT COUNT(*) FROM production_data;")).fetchone()[0]

print(f"📊 Total Sales Rows: {sales_count}")
print(f"📊 Total Production Rows: {production_count}")