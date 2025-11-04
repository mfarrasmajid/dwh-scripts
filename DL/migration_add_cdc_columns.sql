-- Migration script to add missing columns for SAP CDC enhancement
-- Run this on your sap_cdc_registry table

-- Add maxpagesize column (configurable page size for OData requests)
ALTER TABLE sap_cdc_registry 
ADD COLUMN IF NOT EXISTS maxpagesize INTEGER NOT NULL DEFAULT 100000;

COMMENT ON COLUMN sap_cdc_registry.maxpagesize IS 'Max page size for OData requests (odata.maxpagesize header)';

-- Add entity_name column (for constructing DeltaLinksOf path - OPTIONAL, only if delta_path not provided)
ALTER TABLE sap_cdc_registry 
ADD COLUMN IF NOT EXISTS entity_name TEXT NULL DEFAULT NULL;

COMMENT ON COLUMN sap_cdc_registry.entity_name IS 'Entity name for constructing DeltaLinksOf endpoint (OPTIONAL - prefer using delta_path)';

-- OPTIONAL: Remove initial_top column since we no longer use $skip/$top pagination
-- We now use InitialLoad=true with skip tokens, so initial_top is unused
-- Uncomment the line below if you want to drop this column:
-- ALTER TABLE sap_cdc_registry DROP COLUMN IF EXISTS initial_top;

-- IMPORTANT: The delta_path column should already exist in your table
-- Make sure delta_path contains the FULL PATH to the DeltaLinksOf endpoint
-- Example:
--   initial_path: /sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH
--   delta_path:   /sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH

-- Update existing rows to set proper delta_path
-- TEMPLATE (adjust for your tables):
-- UPDATE sap_cdc_registry 
-- SET delta_path = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH'
-- WHERE job_code = 'SAP_AFIH' AND (delta_path IS NULL OR delta_path = initial_path);

-- Note: delta_token, skip_token, and initial_completed_at are stored in state_json JSONB column
-- No additional columns needed - using existing state_json for:
--   - {env}_delta_token: Current delta token from DeltaLinksOf endpoint
--   - {env}_skip_token: Current skip token for pagination continuation
--   - {env}_initial_completed_at: Timestamp when initial load completed (for 10-minute wait)

-- Verify the changes
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'sap_cdc_registry'
AND column_name IN ('maxpagesize', 'entity_name', 'initial_path', 'delta_path', 'state_json')
ORDER BY column_name;
