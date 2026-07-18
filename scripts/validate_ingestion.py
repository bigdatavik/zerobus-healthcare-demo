"""
Validation script to verify Zerobus ingestion results.

Run this on Databricks to query the healthcare_events table and verify data was ingested.
"""

# Query the table to see ingested records
df = spark.sql("""
    SELECT
        event_id,
        member_id,
        event_type,
        from_unixtime(event_timestamp / 1000000) as event_time,
        facility_code,
        diagnosis_code,
        procedure_code,
        amount,
        metadata
    FROM humana_payer.zerobus_demo.healthcare_events
    ORDER BY event_timestamp DESC
    LIMIT 20
""")

print("=" * 80)
print("Healthcare Events - Latest 20 Records")
print("=" * 80)
df.show(truncate=False)

# Get summary statistics
print("\n" + "=" * 80)
print("Summary Statistics")
print("=" * 80)

summary = spark.sql("""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT member_id) as unique_members,
        COUNT(DISTINCT event_type) as event_types,
        MIN(from_unixtime(event_timestamp / 1000000)) as earliest_event,
        MAX(from_unixtime(event_timestamp / 1000000)) as latest_event,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount
    FROM humana_payer.zerobus_demo.healthcare_events
""")
summary.show(truncate=False)

# Event type breakdown
print("\n" + "=" * 80)
print("Event Type Breakdown")
print("=" * 80)

breakdown = spark.sql("""
    SELECT
        event_type,
        COUNT(*) as count,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(SUM(amount), 2) as total_amount
    FROM humana_payer.zerobus_demo.healthcare_events
    GROUP BY event_type
    ORDER BY count DESC
""")
breakdown.show(truncate=False)
