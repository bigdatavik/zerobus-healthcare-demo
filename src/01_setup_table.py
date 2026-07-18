# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create Schema and Table
# MAGIC
# MAGIC This notebook creates the Unity Catalog schema and Delta table for Zerobus ingestion.
# MAGIC
# MAGIC **Run this once before ingesting data.**

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "zerobus_demo", "Schema Name")
dbutils.widgets.text("table", "healthcare_events", "Table Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")

FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

print(f"Setting up: {FULL_TABLE_NAME}")

# COMMAND ----------

# Validate catalog is provided
assert CATALOG, "ERROR: catalog parameter is required"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema created: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
    event_id        STRING      COMMENT 'Unique event identifier (UUID)',
    member_id       STRING      COMMENT 'Member/patient identifier',
    event_type      STRING      COMMENT 'Type: admission, discharge, claim, rx_fill, lab_result',
    event_timestamp BIGINT      COMMENT 'Unix timestamp in microseconds',
    facility_code   STRING      COMMENT 'Healthcare facility code',
    diagnosis_code  STRING      COMMENT 'ICD-10 diagnosis code',
    procedure_code  STRING      COMMENT 'CPT procedure code',
    provider_npi    STRING      COMMENT 'Provider National Provider Identifier',
    amount          DOUBLE      COMMENT 'Event amount in USD',
    metadata        STRING      COMMENT 'JSON string with additional attributes'
)
COMMENT 'Healthcare events ingested via Zerobus for near real-time analytics'
""")

print(f"Table created: {FULL_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table

# COMMAND ----------

# Show table schema
display(spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}"))

# COMMAND ----------

# Show record count
count = spark.sql(f"SELECT COUNT(*) as count FROM {FULL_TABLE_NAME}").collect()[0]["count"]
print(f"Current record count: {count}")

# COMMAND ----------

print(f"""
Setup complete!

Table: {FULL_TABLE_NAME}

Next steps:
1. Create a service principal for Zerobus authentication
2. Grant permissions:

   GRANT USE CATALOG ON CATALOG {CATALOG} TO `<service-principal-id>`;
   GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `<service-principal-id>`;
   GRANT MODIFY, SELECT ON TABLE {FULL_TABLE_NAME} TO `<service-principal-id>`;

3. Run the ingestion job
""")
