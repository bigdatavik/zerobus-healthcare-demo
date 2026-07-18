"""
Zerobus Ingest Producer for Healthcare Events
Ingests sample healthcare data into Delta tables via Zerobus gRPC.

GitHub: https://github.com/bigdatavik/zerobus-healthcare-demo
"""

import json
import time
import uuid
import random
import os

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# =============================================================================
# Configuration - SET THESE VALUES FOR YOUR ENVIRONMENT
# =============================================================================

# Zerobus server endpoint: <workspace_id>.zerobus.<region>.cloud.databricks.com
SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT", "<workspace_id>.zerobus.<region>.cloud.databricks.com")

# Your Databricks workspace URL
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://<your-workspace>.cloud.databricks.com")

# Target table in Unity Catalog (must exist): <catalog>.<schema>.<table>
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "<catalog>.<schema>.healthcare_events")

# Service principal credentials (OAuth M2M)
CLIENT_ID = os.getenv("ZEROBUS_CLIENT_ID", "<your-service-principal-client-id>")
CLIENT_SECRET = os.getenv("ZEROBUS_CLIENT_SECRET", "<your-service-principal-secret>")

# =============================================================================
# Sample Data
# =============================================================================

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
        "source_system": "demo_producer",
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


# =============================================================================
# Main Ingestion
# =============================================================================

def ingest_healthcare_events(num_records: int = 10):
    """Ingest sample healthcare events via Zerobus."""

    print(f"Initializing Zerobus SDK...")
    print(f"  Server: {SERVER_ENDPOINT}")
    print(f"  Table: {TABLE_NAME}")
    print()

    # Initialize SDK
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    table_props = TableProperties(TABLE_NAME)

    # Create stream (connects to Zerobus)
    print("Creating stream...")
    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)

    try:
        print(f"Ingesting {num_records} events...")
        print("-" * 60)

        successful = 0
        failed = 0

        for i in range(num_records):
            record = generate_healthcare_event()

            try:
                # Send record and wait for ACK
                ack = stream.ingest_record(record)
                ack.wait_for_ack()

                successful += 1
                print(f"  [{i+1}/{num_records}] {record['event_type']:12} | {record['member_id']} | ${record['amount']:,.2f}")

            except Exception as e:
                failed += 1
                print(f"  [{i+1}/{num_records}] FAILED: {e}")

        print("-" * 60)
        print(f"Done! Successful: {successful}, Failed: {failed}")

    finally:
        stream.close()
        print("Stream closed.")


# Run
if __name__ == "__main__":
    ingest_healthcare_events(num_records=10)
