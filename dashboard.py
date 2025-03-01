import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

st.set_page_config(page_title="Brewery Dashboard", layout="wide")

# Set the default port or use environment variable
port = int(os.getenv("PORT", 5000))  # Use provided port or default to 5000

# Run Streamlit
if __name__ == "__main__":
    st.write("✅ Connected to Database!")
    st.write("🍺 Brewery Dashboard is running!")

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL)

# Load Data from PostgreSQL
@st.cache_data
def load_data():
    sales_query = "SELECT * FROM sales_summary;"
    production_query = "SELECT * FROM production_summary;"
    profitability_query = "SELECT * FROM profitability_summary;"
    
    sales_df = pd.read_sql(sales_query, engine)
    production_df = pd.read_sql(production_query, engine)
    profitability_df = pd.read_sql(profitability_query, engine)
    
    return sales_df, production_df, profitability_df

# Load Data
sales_df, production_df, profitability_df = load_data()

# Streamlit Dashboard UI
st.title("Brewery Sales & Production Dashboard")

# Sales Overview
st.subheader("Sales Revenue by Product & Region")
st.dataframe(sales_df)

# Sales Chart
st.bar_chart(sales_df.groupby("product")["total_revenue"].sum())

# Production Overview
st.subheader("Production Efficiency")
st.dataframe(production_df)

# Efficiency Chart
st.line_chart(production_df.groupby("product")["avg_efficiency"].mean())

# Profitability Overview
st.subheader("Profitability Analysis")
st.dataframe(profitability_df)

# Profit Chart
st.bar_chart(profitability_df.groupby("product")["profit"].sum())

st.success("✅ Data Updated Successfully!")
