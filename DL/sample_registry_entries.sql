-- Sample registry entry for SAP CDC with new enhancements
-- This demonstrates how to configure a job for the new CDC implementation

-- Example 1: Continuous CDC for SAP AFIH table
INSERT INTO sap_cdc_registry (
    job_code,
    service_name,
    entity_name,          -- OPTIONAL: Only if delta_path not provided
    method,
    schedule_type,
    interval_minutes,
    initial_path,
    delta_path,           -- IMPORTANT: Full path to DeltaLinksOf endpoint
    maxpagesize,          -- NEW: Configurable page size (no more initial_top)
    
    -- SIM environment
    
    -- SIM environment
    sim_enabled,
    sim_db,
    sim_table,
    sim_client,
    
    -- QAS environment
    qas_enabled,
    qas_db,
    qas_table,
    qas_client,
    
    -- PROD environment
    prod_enabled,
    prod_db,
    prod_table,
    prod_client,
    
    -- Connections
    ck_conn_id,
    log_conn_id,
    
    is_enabled
) VALUES (
    'SAP_AFIH_CDC',
    'ZCDC_AFIH_1_SRV',
    'ZCDC_AFIH_1',        -- Only used if delta_path is NULL or same as initial_path
    'continuous_cdc',
    'interval',
    2,                    -- Run every 2 minutes
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH',
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH',  -- Full DeltaLinksOf path
    100000,               -- Max 100k records per page (no more initial_top)
    
    -- SIM
    true,
    'dl_sim',
    'sap_afih',
    '300',
    
    -- QAS
    true,
    'dl_qas',
    'sap_afih',
    '300',
    
    -- PROD
    true,
    'dl_prod',
    'sap_afih',
    '300',
    
    'clickhouse_default',
    'airflow_logs_mitratel',
    
    true
);

-- Example 2: Weekly refresh for SAP table
INSERT INTO sap_cdc_registry (
    job_code,
    service_name,
    entity_name,
    method,
    schedule_type,
    cron_expr,            -- Use cron for weekly schedule
    initial_path,
    delta_path,
    maxpagesize,
    
    sim_enabled,
    sim_db,
    sim_table,
    sim_client,
    
    qas_enabled,
    qas_db,
    qas_table,
    qas_client,
    
    prod_enabled,
    prod_db,
    prod_table,
    prod_client,
    
    is_enabled
) VALUES (
    'SAP_EKPO_WEEKLY',
    'ZCDC_EKPO_SRV',
    'ZCDC_EKPO',
    'weekly_refresh',
    'cron',
    '0 0 * * 0',          -- Every Sunday at midnight
    '/sap/opu/odata/sap/ZCDC_EKPO_SRV/FactsOfZCDCEKPO',
    '/sap/opu/odata/sap/ZCDC_EKPO_SRV/DeltaLinksOfFactsOfZCDCEKPO',  -- Not used in weekly_refresh but good to have
    50000,                -- Smaller page size (no more initial_top)
    
    true,
    'dl_sim',
    'sap_ekpo',
    '300',
    
    true,
    'dl_qas',
    'sap_ekpo',
    '300',
    
    true,
    'dl_prod',
    'sap_ekpo',
    '300',
    
    true
);

-- Example 3: Update existing job to add new columns
UPDATE sap_cdc_registry
SET 
    maxpagesize = 100000,
    entity_name = 'ZCDC_ZDMT_MMTB020_2',
    initial_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020',
    delta_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020'
WHERE job_code = 'SAP_ZDMT_MMTB020';

-- Verify the configuration
SELECT 
    job_code,
    service_name,
    entity_name,
    method,
    maxpagesize,
    initial_path,
    sim_enabled,
    sim_table,
    is_enabled
FROM sap_cdc_registry
WHERE job_code LIKE 'SAP_%'
ORDER BY job_code;

-- Check state_json for a specific job (to see tokens)
SELECT 
    job_code,
    sim_initial_done,
    qas_initial_done,
    prod_initial_done,
    state_json
FROM sap_cdc_registry
WHERE job_code = 'SAP_AFIH_CDC';

-- Force initial load for a job (clears all CDC state)
UPDATE sap_cdc_registry
SET 
    trigger_force_initial = true,
    trigger_run_now = true
WHERE job_code = 'SAP_AFIH_CDC';

-- Sample state_json structure (auto-populated by DAG):
-- {
--   "pk_cols": ["AUFNR", "AUFPL"],
--   "sim_delta_token": "D20251104043043_000037000",
--   "sim_skip_token": null,
--   "sim_initial_completed_at": "2025-11-04T10:30:00",
--   "qas_delta_token": "D20251104050000_000040000",
--   "qas_skip_token": "D20251104050000_000040000_0000000001",
--   "qas_initial_completed_at": "2025-11-04T11:00:00"
-- }
