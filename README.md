# Brewery Data PoC

A proof of concept data pipeline and dashboard for a brewery that processes sales and production data.

## Features

- Data ingestion from CSV files to PostgreSQL
- Data transformation with summary tables and analytics
- Interactive dashboard for data visualization

## Setup

### Prerequisites

- Python 3.6+
- PostgreSQL database

### Installation

1. Clone this repository
2. Install the required packages:
```
pip install -r requirements.txt
```

### Environment Variables

This project uses environment variables for configuration. Create a `.env` file in the root directory with the following variables:

```
DATABASE_URL = "postgresql://username:password@hostname:port/database?sslmode=require"
```

A template `.env.example` file is provided for reference.


## Usage

### Data Ingestion

Load the mock data into the PostgreSQL database:

```
python ingest_data.py
```

### Data Transformation

Transform the raw data into summary tables:

```
python transform_data.py
```

### Dashboard

Run the interactive dashboard:

```
streamlit run dashboard.py
```

## File Structure

- `ingest_data.py` - Loads CSV data into PostgreSQL
- `transform_data.py` - Creates summary tables and analytics
- `dashboard.py` - Streamlit dashboard for data visualization
- `mock_sales_data.csv` - Sample sales data
- `mock_production_data.csv` - Sample production data
- `requirements.txt` - Python dependencies
- `.env.example` - Example environment variables file 