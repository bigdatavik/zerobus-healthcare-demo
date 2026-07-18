# Zerobus Healthcare Demo

Real-time healthcare data ingestion into Databricks Delta Lake using Zerobus — no Kafka required.

## What is Zerobus?

Zerobus is Databricks' serverless streaming ingestion service that lets you write data directly to Delta tables via gRPC. No message bus infrastructure needed.

```
Traditional:  Source → Kafka → Spark Streaming → Delta Lake
Zerobus:      Source → Zerobus (gRPC) → Delta Lake
```

## Quick Start with Databricks Asset Bundles

The easiest way to deploy this demo is using Databricks Asset Bundles (DAB).

### 1. Prerequisites

- Databricks workspace with Unity Catalog
- [Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/install.html)
- Python 3.8+

### 2. Clone the Repository

```bash
git clone https://github.com/bigdatavik/zerobus-healthcare-demo.git
cd zerobus-healthcare-demo
```

### 3. Create the Target Table

Run this SQL in your Databricks workspace (SQL Editor or notebook):

```sql
-- Replace <catalog> with your catalog name
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

### 4. Create a Service Principal

```bash
# Create service principal
databricks service-principals create --display-name "zerobus-producer"
# Note the applicationId → CLIENT_ID

# Generate OAuth secret
databricks service-principals secrets create <service-principal-id>
# Note the secret → CLIENT_SECRET
```

Grant permissions:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.zerobus_demo TO `<service-principal-id>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.zerobus_demo.healthcare_events TO `<service-principal-id>`;
```

### 5. Configure the Bundle

Edit `databricks.yml` and update:

```yaml
variables:
  zerobus_server_endpoint:
    default: "1234567890.zerobus.us-east-1.cloud.databricks.com"  # Your workspace_id

  table_name:
    default: "my_catalog.zerobus_demo.healthcare_events"  # Your table

  client_id:
    default: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # Service principal ID

  client_secret:
    default: "dosexxxxxxxxxxxxxxxxxxxxxxxxx"  # Service principal secret

targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com  # Your workspace URL
```

### 6. Deploy and Run

```bash
# Authenticate to your workspace
databricks auth login --host https://your-workspace.cloud.databricks.com

# Validate the bundle
databricks bundle validate

# Deploy to your workspace
databricks bundle deploy

# Run the job
databricks bundle run zerobus_healthcare_ingest
```

### 7. View Results

After the job completes, query your table:

```sql
SELECT * FROM <catalog>.zerobus_demo.healthcare_events
ORDER BY event_timestamp DESC
LIMIT 10;
```

---

## Alternative: Manual Deployment

If you prefer not to use Asset Bundles:

### Option A: Run in Databricks Notebook

1. Import `scripts/zerobus_ingest_notebook.ipynb` into your workspace
2. Update the configuration cell with your values
3. Attach to a cluster and run all cells

### Option B: Run Locally

```bash
pip install databricks-zerobus-ingest-sdk

export ZEROBUS_SERVER_ENDPOINT="<workspace_id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="<catalog>.zerobus_demo.healthcare_events"
export ZEROBUS_CLIENT_ID="<your-client-id>"
export ZEROBUS_CLIENT_SECRET="<your-client-secret>"

python scripts/zerobus_ingest.py
```

---

## Project Structure

```
zerobus-healthcare-demo/
├── databricks.yml                 # Bundle configuration (EDIT THIS)
├── resources/
│   └── zerobus_job.yml            # Job definition
├── src/
│   └── zerobus_ingest_job.py      # Job notebook (uses parameters)
├── scripts/
│   ├── zerobus_ingest.py          # Standalone script
│   ├── zerobus_ingest_notebook.ipynb  # Interactive notebook
│   └── setup_table.sql            # Table creation SQL
└── README.md
```

---

## Configuration Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `zerobus_server_endpoint` | Zerobus endpoint | `1234567890.zerobus.us-east-1.cloud.databricks.com` |
| `table_name` | Target Unity Catalog table | `my_catalog.zerobus_demo.healthcare_events` |
| `client_id` | Service principal application ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `client_secret` | Service principal OAuth secret | `dosexxxxxxxxxxxxxxxxxxxxxxxxx` |
| `num_records` | Records to generate per run | `10` |

### Finding Your Workspace ID

Your workspace ID is in the URL when you're logged into Databricks:
```
https://<workspace-name>.cloud.databricks.com/?o=<workspace_id>
```

The Zerobus endpoint is: `<workspace_id>.zerobus.<region>.cloud.databricks.com`

---

## Healthcare Use Cases

This demo generates sample healthcare events:

| Event Type | Description | Amount Range |
|------------|-------------|--------------|
| `admission` | Patient admitted to facility | $5,000 - $50,000 |
| `discharge` | Patient discharged | $0 - $500 |
| `claim` | Insurance claim submitted | $100 - $10,000 |
| `rx_fill` | Prescription filled | $10 - $500 |
| `lab_result` | Lab test completed | $50 - $1,000 |

---

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

---

## Bundle Commands Reference

```bash
# Validate configuration
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run the job
databricks bundle run zerobus_healthcare_ingest

# Destroy deployed resources
databricks bundle destroy
```

---

## Documentation

- [Zerobus Overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
- [Zerobus SDK Reference](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)

## License

MIT
