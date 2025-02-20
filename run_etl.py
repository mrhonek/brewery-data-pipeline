import os
import subprocess

# Get the script name from the environment variable
task = os.getenv("ETL_TASK", "ingest")

if task == "ingest":
    print("✅ Running ingest_data.py...")
    subprocess.run(["python", "ingest_data.py"], check=True)
elif task == "transform":
    print("✅ Running transform_data.py...")
    subprocess.run(["python", "transform_data.py"], check=True)
else:
    print("❌ ERROR: Invalid ETL_TASK value. Use 'ingest' or 'transform'.")
    exit(1)
