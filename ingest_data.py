import pandas as pd
from sqlalchemy import create_engine

# Railway PostgreSQL connection string
DATABASE_URL = "postgresql://postgres:mcJRZJIrRCCMwwXDrRADmBMsyiGRVkRH@postgres.railway.internal:5432/railway"

# Create database connection
engine = create_engine(DATABASE_URL)

# Create Tables if not exist
with engine.connect() as connection:
    connection.execute("""
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            date DATE,
            product VARCHAR(50),
            region VARCHAR(50),
            units_sold INT,
            price_per_unit DECIMAL(5,2)
        );
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS production_data (
            id SERIAL PRIMARY KEY,
            date DATE,
            product VARCHAR(50),
            units_produced INT,
            spoiled_units INT,
            cost_per_unit DECIMAL(5,2)
        );
    """)

print("✅ Tables created successfully.")

# Load CSV Data
sales_df = pd.read_csv("mock_sales_data.csv")
production_df = pd.read_csv("mock_production_data.csv")

# Insert Data into PostgreSQL
sales_df.to_sql("sales_data", engine, if_exists="append", index=False)
production_df.to_sql("production_data", engine, if_exists="append", index=False)

print("✅ Data successfully loaded into PostgreSQL.")
