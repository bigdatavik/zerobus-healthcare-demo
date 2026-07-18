# Zerobus Healthcare Demo

Real-time healthcare data ingestion into Databricks Delta Lake using Zerobus — **no Kafka required**.

```
Traditional:  Source → Kafka → Spark Streaming → Delta Lake  (Complex)
Zerobus:      Source → Zerobus (gRPC) → Delta Lake            (Simple)
```

## What This Demo Does

1. **Creates a Delta table** in Unity Catalog
2. **Ingests sample healthcare events** (admissions, claims, lab results, prescriptions)
3. **Provides an interactive notebook** for demos and learning

Everything deploys with a single command using Databricks Asset Bundles.

---

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog
- [Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/install.html)

### Step 1: Clone the Repository

```bash
git clone https://github.com/bigdatavik/zerobus-healthcare-demo.git
cd zerobus-healthcare-demo
```

### Step 2: Create a Service Principal

```bash
# Authenticate to your workspace
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com

# Create service principal
databricks service-principals create --display-name "zerobus-producer"
# → Note the applicationId (this is your CLIENT_ID)

# Generate OAuth secret
databricks service-principals secrets create <APPLICATION_ID>
# → Note the secret (this is your CLIENT_SECRET)
```

### Step 3: Configure the Bundle

Copy the sample config and edit it:

```bash
cp databricks.yml.sample databricks.yml
```

Edit `databricks.yml` with your values:

```yaml
variables:
  catalog:
    default: "my_catalog"                    # Your Unity Catalog

  zerobus_server_endpoint:
    default: "123456789.zerobus.us-east-1.cloud.databricks.com"  # Your workspace_id

  client_id:
    default: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # From Step 2

  client_secret:
    default: "dosexxxxxxxxxxxxxxxxxxxxxxxxx"        # From Step 2

targets:
  dev:
    workspace:
      host: "https://my-workspace.cloud.databricks.com"  # Your workspace URL
```

**Finding your workspace_id:**
Look at your URL: `https://xxx.cloud.databricks.com/?o=<workspace_id>`

### Step 4: Deploy

```bash
# Validate configuration
databricks bundle validate

# Deploy everything (creates table + job + notebooks)
databricks bundle deploy
```

### Step 5: Grant Permissions

Run this SQL in your workspace (SQL Editor):

```sql
-- Replace with your values
GRANT USE CATALOG ON CATALOG <catalog> TO `<client_id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.zerobus_demo TO `<client_id>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.zerobus_demo.healthcare_events TO `<client_id>`;
```

### Step 6: Run the Job

```bash
databricks bundle run zerobus_healthcare_ingest
```

This will:
1. Create the schema and table (if not exists)
2. Ingest 10 sample healthcare events
3. Display validation results

### Step 7: Explore the Data

```sql
SELECT * FROM <catalog>.zerobus_demo.healthcare_events
ORDER BY event_timestamp DESC
LIMIT 10;
```

---

## Project Structure

```
zerobus-healthcare-demo/
├── databricks.yml           # Bundle config (EDIT THIS)
├── databricks.yml.sample    # Sample config with examples
├── resources/
│   └── zerobus_job.yml      # Job definition (2 tasks)
├── src/
│   ├── 01_setup_table.py    # Creates schema & table
│   ├── 02_zerobus_ingest.py # Ingests data via Zerobus
│   └── zerobus_interactive.ipynb  # Interactive demo notebook
└── README.md
```

---

## Interactive Demo Notebook

After deployment, find the interactive notebook at:

```
/Workspace/Users/<your-email>/.bundle/zerobus-healthcare-demo/dev/files/src/zerobus_interactive.ipynb
```

This notebook includes:
- Architecture diagrams
- Step-by-step code with explanations
- Real-world vs demo comparison
- Table reset functionality

---

## Healthcare Use Cases

| Event Type | Description | Amount Range |
|------------|-------------|--------------|
| `admission` | Patient admitted | $5,000 - $50,000 |
| `discharge` | Patient discharged | $0 - $500 |
| `claim` | Insurance claim | $100 - $10,000 |
| `rx_fill` | Prescription filled | $10 - $500 |
| `lab_result` | Lab test completed | $50 - $1,000 |

---

## The 5 Lines That Matter

```python
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)      # Initialize
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table, options)  # Connect
ack = stream.ingest_record(record)                    # Send
ack.wait_for_ack()                                    # Confirm durability
stream.close()                                        # Cleanup
```

---

## Bundle Commands

```bash
databricks bundle validate    # Check configuration
databricks bundle deploy      # Deploy to workspace
databricks bundle run <job>   # Run a job
databricks bundle destroy     # Remove all resources
```

---

## Documentation

- [Zerobus Overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
- [Zerobus SDK](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)

---

## License

MIT
