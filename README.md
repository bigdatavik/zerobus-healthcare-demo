# Zerobus Healthcare Demo

Real-time healthcare data ingestion into Databricks Delta Lake using Zerobus — no Kafka required.

## What is Zerobus?

Zerobus is Databricks' serverless streaming ingestion service that lets you write data directly to Delta tables via gRPC. No message bus infrastructure needed.

```
Traditional:  Source → Kafka → Spark Streaming → Delta Lake
Zerobus:      Source → Zerobus (gRPC) → Delta Lake
```

## Quick Start

### 1. Prerequisites

- Databricks workspace with Unity Catalog
- Python 3.8+
- Databricks CLI configured

### 2. Create the Target Table

Run the SQL in `scripts/setup_table.sql` in your Databricks workspace:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog>.zerobus_demo;

CREATE TABLE IF NOT EXISTS <catalog>.zerobus_demo.healthcare_events (
    event_id        STRING,
    member_id       STRING,
    event_type      STRING,
    event_timestamp BIGINT,
    facility_code   STRING,
    diagnosis_code  STRING,
    procedure_code  STRING,
    provider_npi    STRING,
    amount          DOUBLE,
    metadata        STRING
);
```

### 3. Create a Service Principal

```bash
# Create service principal
databricks service-principals create --display-name "zerobus-producer"

# Note the applicationId - this is your CLIENT_ID

# Generate OAuth secret
databricks service-principals secrets create <service-principal-id>

# Note the secret - this is your CLIENT_SECRET
```

### 4. Grant Permissions

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.zerobus_demo TO `<service-principal-id>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.zerobus_demo.healthcare_events TO `<service-principal-id>`;
```

### 5. Configure and Run

**Option A: Run in Databricks Notebook**

1. Import `scripts/zerobus_ingest_notebook.ipynb` into your workspace
2. Update the configuration cell with your values
3. Run all cells

**Option B: Run as Databricks Job**

1. Import `scripts/zerobus_ingest.py` into your workspace
2. Create a job with serverless compute
3. Add `databricks-zerobus-ingest-sdk>=0.2.0` to environment dependencies
4. Set environment variables or update the script with your configuration

**Option C: Run Locally (requires network access to Databricks)**

```bash
pip install databricks-zerobus-ingest-sdk

export ZEROBUS_SERVER_ENDPOINT="<workspace_id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="<catalog>.zerobus_demo.healthcare_events"
export ZEROBUS_CLIENT_ID="<your-client-id>"
export ZEROBUS_CLIENT_SECRET="<your-client-secret>"

python scripts/zerobus_ingest.py
```

## Configuration Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `SERVER_ENDPOINT` | Zerobus endpoint | `1234567890.zerobus.us-east-1.cloud.databricks.com` |
| `WORKSPACE_URL` | Databricks workspace URL | `https://myworkspace.cloud.databricks.com` |
| `TABLE_NAME` | Target Unity Catalog table | `my_catalog.zerobus_demo.healthcare_events` |
| `CLIENT_ID` | Service principal application ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `CLIENT_SECRET` | Service principal OAuth secret | `dosexxxxxxxxxxxxxxxxxxxxxxxxx` |

### Finding Your Workspace ID

Your workspace ID is in the URL when you're logged into Databricks:
```
https://<workspace-name>.cloud.databricks.com/?o=<workspace_id>
```

Or run:
```bash
databricks workspace get-status /
```

## Project Structure

```
zerobus_demo/
├── README.md                              # This file
├── scripts/
│   ├── zerobus_ingest.py                  # Producer script (for jobs)
│   ├── zerobus_ingest_notebook.ipynb      # Interactive notebook (for learning)
│   └── setup_table.sql                    # Table creation SQL
└── .gitignore
```

## Healthcare Use Cases

This demo generates sample healthcare events:

| Event Type | Description | Amount Range |
|------------|-------------|--------------|
| `admission` | Patient admitted to facility | $5,000 - $50,000 |
| `discharge` | Patient discharged | $0 - $500 |
| `claim` | Insurance claim submitted | $100 - $10,000 |
| `rx_fill` | Prescription filled | $10 - $500 |
| `lab_result` | Lab test completed | $50 - $1,000 |

## Key Concepts

### The 5 Lines That Matter

```python
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)      # Initialize
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)  # Connect
ack = stream.ingest_record(record)                    # Send
ack.wait_for_ack()                                    # Confirm
stream.close()                                        # Cleanup
```

### Durability Guarantee

After `wait_for_ack()` returns:
- Record is written to Delta Lake
- Immediately queryable
- Safe even if Zerobus crashes

### Important Data Types

- `event_timestamp`: Must be `BIGINT` (microseconds since epoch)
- `metadata`: Must be a JSON string, not a dict

## Documentation

- [Zerobus Overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
- [Zerobus SDK Reference](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
- [Zerobus Limits](https://docs.databricks.com/aws/en/ingestion/zerobus-limits)

## License

MIT
