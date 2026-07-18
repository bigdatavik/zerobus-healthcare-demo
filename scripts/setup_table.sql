-- ============================================================================
-- Zerobus Demo Setup Script
-- Healthcare Events Table and Permissions
-- ============================================================================

-- Create the schema (update <catalog> to your catalog name)
CREATE SCHEMA IF NOT EXISTS <catalog>.zerobus_demo;

-- Create the healthcare events table
CREATE TABLE IF NOT EXISTS <catalog>.zerobus_demo.healthcare_events (
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
COMMENT 'Healthcare events ingested via Zerobus for near real-time analytics';

-- Grant permissions to the Zerobus service principal
-- Replace <service-principal-id> with your service principal's application ID

GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.zerobus_demo TO `<service-principal-id>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.zerobus_demo.healthcare_events TO `<service-principal-id>`;

-- Verify table exists
DESCRIBE TABLE EXTENDED <catalog>.zerobus_demo.healthcare_events;
