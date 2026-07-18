# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus Healthcare Ingest
# MAGIC
# MAGIC Ingests sample healthcare events into Delta Lake via Zerobus gRPC.

# COMMAND ----------

# Get parameters from job configuration
dbutils.widgets.text("zerobus_server_endpoint", "", "Zerobus Server Endpoint")
dbutils.widgets.text("workspace_url", "", "Workspace URL")
dbutils.widgets.text("table_name", "", "Target Table")
dbutils.widgets.text("client_id", "", "Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")
dbutils.widgets.text("num_records", "10", "Number of Records")

SERVER_ENDPOINT = dbutils.widgets.get("zerobus_server_endpoint")
WORKSPACE_URL = dbutils.widgets.get("workspace_url")
TABLE_NAME = dbutils.widgets.get("table_name")
CLIENT_ID = dbutils.widgets.get("client_id")
CLIENT_SECRET = dbutils.widgets.get("client_secret")
NUM_RECORDS = int(dbutils.widgets.get("num_records"))

print(f"Server Endpoint: {SERVER_ENDPOINT}")
print(f"Workspace URL: {WORKSPACE_URL}")
print(f"Target Table: {TABLE_NAME}")
print(f"Records to ingest: {NUM_RECORDS}")

# COMMAND ----------

# Validate configuration
assert SERVER_ENDPOINT, "ERROR: zerobus_server_endpoint is not configured"
assert WORKSPACE_URL, "ERROR: workspace_url is not configured"
assert TABLE_NAME, "ERROR: table_name is not configured"
assert CLIENT_ID, "ERROR: client_id is not configured"
assert CLIENT_SECRET, "ERROR: client_secret is not configured"

print("Configuration validated!")

# COMMAND ----------

import json
import time
import uuid
import random

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Generation

# COMMAND ----------

EVENT_TYPES = ["admission", "discharge", "claim", "rx_fill", "lab_result"]
FACILITY_CODES = ["FAC001", "FAC002", "FAC003", "FAC004", "FAC005"]
DIAGNOSIS_CODES = ["E11.9", "I10", "J06.9", "M54.5", "K21.0", "F32.9", "J45.909"]
PROCEDURE_CODES = ["99213", "99214", "99215", "99203", "99204", "90834", "96372"]
PROVIDER_NPIS = ["1234567890", "2345678901", "3456789012", "4567890123", "5678901234"]


def generate_healthcare_event():
    """Generate a single sample healthcare event."""
    event_type = random.choice(EVENT_TYPES)

    amount_ranges = {
        "admission": (5000, 50000),
        "discharge": (0, 500),
        "claim": (100, 10000),
        "rx_fill": (10, 500),
        "lab_result": (50, 1000)
    }
    min_amt, max_amt = amount_ranges.get(event_type, (100, 1000))

    metadata = {
        "source_system": "zerobus_demo",
        "ingestion_batch": str(uuid.uuid4())[:8]
    }

    if event_type == "admission":
        metadata["admission_type"] = random.choice(["emergency", "elective", "urgent"])
    elif event_type == "rx_fill":
        metadata["ndc_code"] = f"{random.randint(10000, 99999)}-{random.randint(100, 999)}-{random.randint(10, 99)}"
    elif event_type == "lab_result":
        metadata["test_type"] = random.choice(["CBC", "BMP", "HbA1c", "Lipid Panel"])

    return {
        "event_id": str(uuid.uuid4()),
        "member_id": f"MBR{random.randint(100000, 999999)}",
        "event_type": event_type,
        "event_timestamp": int(time.time() * 1_000_000),
        "facility_code": random.choice(FACILITY_CODES),
        "diagnosis_code": random.choice(DIAGNOSIS_CODES),
        "procedure_code": random.choice(PROCEDURE_CODES),
        "provider_npi": random.choice(PROVIDER_NPIS),
        "amount": round(random.uniform(min_amt, max_amt), 2),
        "metadata": json.dumps(metadata)
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Zerobus and Ingest

# COMMAND ----------

print("Initializing Zerobus SDK...")
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
options = StreamConfigurationOptions(record_type=RecordType.JSON)
table_props = TableProperties(TABLE_NAME)

print("Creating stream...")
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)
print("Connected!")

# COMMAND ----------

print(f"Ingesting {NUM_RECORDS} healthcare events...")
print("-" * 60)

successful = 0
failed = 0

for i in range(NUM_RECORDS):
    record = generate_healthcare_event()

    try:
        ack = stream.ingest_record(record)
        ack.wait_for_ack()

        successful += 1
        print(f"  [{i+1}/{NUM_RECORDS}] {record['event_type']:12} | {record['member_id']} | ${record['amount']:,.2f}")

    except Exception as e:
        failed += 1
        print(f"  [{i+1}/{NUM_RECORDS}] FAILED: {e}")

print("-" * 60)
print(f"Ingestion complete! Successful: {successful}, Failed: {failed}")

# COMMAND ----------

stream.close()
print("Stream closed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) as total_records FROM {TABLE_NAME}"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_type, COUNT(*) as count, ROUND(AVG(amount), 2) as avg_amount
    FROM {TABLE_NAME}
    GROUP BY event_type
    ORDER BY count DESC
"""))
