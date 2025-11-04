# Trigger Run Now - Manual Job Execution

## Problem
When `next_run` is NULL or you want to run a job immediately without waiting for the schedule, the job won't execute automatically.

## Solution
Use the `trigger_run_now` column to force immediate execution.

## Quick Start

### Run a Single Job Now
```sql
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    updated_at = NOW()
WHERE job_code = 'YOUR_JOB_CODE';
```

### Run Multiple Jobs Now
```sql
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    updated_at = NOW()
WHERE job_code IN ('JOB1', 'JOB2', 'JOB3');
```

### Run All Active Jobs
```sql
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    updated_at = NOW()
WHERE active = true;
```

## How It Works

### 1. Set the Trigger
```sql
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';
```

### 2. Wait for Next DAG Run
The Airflow DAG runs every 2 minutes (default schedule: `*/2 * * * *`)

### 3. Job Executes
- Script reads `trigger_run_now = TRUE`
- **Bypasses** the `next_run` schedule check
- Executes immediately
- Logs: `"Job AFIH env PROD run_now=True (ignoring schedule)"`

### 4. Auto-Reset
After job completes:
- Script automatically sets `trigger_run_now = FALSE`
- Job returns to normal schedule

## Common Scenarios

### Scenario 1: NULL next_run (First Time Setup)
```sql
-- Job has never run, next_run is NULL
SELECT job_code, prod_next_run, qas_next_run 
FROM sap_cdc_registry 
WHERE job_code = 'AFIH';

-- Result: prod_next_run = NULL, qas_next_run = NULL

-- Trigger immediate run
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';
```

### Scenario 2: Next Run is in the Future
```sql
-- Job is scheduled for tomorrow
SELECT job_code, prod_next_run 
FROM sap_cdc_registry 
WHERE job_code = 'AFIH';

-- Result: prod_next_run = '2025-11-05 10:00:00'

-- But you need it NOW
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';

-- Will run immediately without waiting until tomorrow
```

### Scenario 3: Job is Disabled (next_run NULL due to manual stop)
```sql
-- Job was manually stopped
UPDATE sap_cdc_registry
SET prod_next_run = NULL
WHERE job_code = 'AFIH';

-- Now restart it
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    prod_next_run = NOW()  -- Optional: set a next_run too
WHERE job_code = 'AFIH';
```

### Scenario 4: Force Immediate Re-run (Just Ran 1 Minute Ago)
```sql
-- Job ran 1 minute ago, next_run is 1 minute in the future
-- But you need to run it again NOW

UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';

-- Will ignore the schedule and run immediately
```

## Combined with Other Triggers

### Force Initial Load + Run Now
```sql
-- Clear state and run initial load immediately
UPDATE sap_cdc_registry
SET 
    trigger_force_initial = TRUE,  -- Force initial load
    trigger_run_now = TRUE,        -- Run immediately
    state_json = jsonb_set(
        COALESCE(state_json, '{}'),
        '{prod_skip_token}',
        'null'
    ),
    updated_at = NOW()
WHERE job_code = 'AFIH';
```

### Weekly Refresh + Run Now
```sql
-- Trigger weekly refresh immediately
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    updated_at = NOW()
WHERE 
    job_code = 'AFIH'
    AND cdc_method = 'weekly_refresh';
```

## Monitoring

### Check if Trigger is Set
```sql
SELECT 
    job_code,
    trigger_run_now,
    trigger_force_initial,
    prod_next_run,
    qas_next_run,
    prod_last_run,
    prod_last_status
FROM sap_cdc_registry
WHERE trigger_run_now = TRUE;
```

### Watch for Execution
```sql
-- Before execution
SELECT job_code, trigger_run_now, prod_last_run
FROM sap_cdc_registry
WHERE job_code = 'AFIH';
-- Result: trigger_run_now = TRUE, prod_last_run = '2025-11-04 10:00:00'

-- After execution (wait 2 minutes for DAG to run)
SELECT job_code, trigger_run_now, prod_last_run
FROM sap_cdc_registry
WHERE job_code = 'AFIH';
-- Result: trigger_run_now = FALSE, prod_last_run = '2025-11-04 10:02:00' (updated!)
```

### Airflow Logs
Look for this message:
```
[2025-11-04 10:02:00] Job AFIH env PROD run_now=True (ignoring schedule)
```

## Important Notes

### 1. Auto-Reset
The script **automatically resets** `trigger_run_now = FALSE` after each run. You don't need to manually reset it.

### 2. One-Time Trigger
`trigger_run_now = TRUE` only affects the **next** DAG run. After that run completes, it resets to FALSE.

### 3. Works for All Environments
If you have both PROD and QAS:
```sql
-- Both environments will run
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';

-- This will trigger:
-- 1. PROD environment run
-- 2. QAS environment run
```

### 4. Respects active=false
Even with `trigger_run_now = TRUE`, if `active = FALSE`, the job won't run:
```sql
-- This WON'T run because active=false
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    active = FALSE  -- Job is disabled!
WHERE job_code = 'AFIH';

-- Fix: Enable the job first
UPDATE sap_cdc_registry
SET 
    trigger_run_now = TRUE,
    active = TRUE  -- Enable it!
WHERE job_code = 'AFIH';
```

## Troubleshooting

### Problem: Set trigger_run_now but job didn't run

**Check 1: Is the job active?**
```sql
SELECT job_code, active, trigger_run_now
FROM sap_cdc_registry
WHERE job_code = 'AFIH';
```
- If `active = FALSE`, set it to TRUE

**Check 2: Is the DAG running?**
- Check Airflow UI - is `DL_sap_all_table` DAG enabled?
- Check last DAG run time

**Check 3: Check for errors**
```sql
SELECT 
    job_code,
    prod_last_status,
    prod_last_error,
    prod_last_run
FROM sap_cdc_registry
WHERE job_code = 'AFIH';
```

**Check 4: Was trigger reset?**
```sql
-- If trigger_run_now is still TRUE after 5 minutes, 
-- the DAG might not be running
SELECT 
    job_code, 
    trigger_run_now,
    updated_at,
    NOW() - updated_at as time_since_trigger
FROM sap_cdc_registry
WHERE trigger_run_now = TRUE;
```

### Problem: Job ran but didn't process data

This is a **schedule** trigger, not a data trigger. The job will:
1. Run immediately ✅
2. Check if it's time to pull data (based on CDC logic)
3. Pull data if conditions are met

If no data was pulled, check:
- Initial load status: `prod_initial_done`
- Last delta pull time
- Skip tokens in `state_json`

## Examples

### Example 1: New Job, Never Run
```sql
-- Setup new job
INSERT INTO sap_cdc_registry (
    job_code, initial_path, delta_path, 
    cdc_method, interval_minutes, active
) VALUES (
    'NEW_JOB', '/path/to/entity', '/path/to/delta',
    'continuous_cdc', 2, TRUE
);

-- Trigger first run immediately
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'NEW_JOB';

-- Wait 2 minutes, check result
SELECT 
    job_code,
    trigger_run_now,  -- Should be FALSE now
    prod_last_run,     -- Should have a timestamp
    prod_last_status,  -- Should be 'success' or 'error'
    prod_next_run      -- Should have next scheduled time
FROM sap_cdc_registry
WHERE job_code = 'NEW_JOB';
```

### Example 2: Resume Stopped Job
```sql
-- Job was stopped (next_run = NULL)
UPDATE sap_cdc_registry
SET 
    prod_next_run = NULL,
    active = FALSE
WHERE job_code = 'STOPPED_JOB';

-- Resume it
UPDATE sap_cdc_registry
SET 
    active = TRUE,
    trigger_run_now = TRUE
WHERE job_code = 'STOPPED_JOB';
```

### Example 3: Test Job Configuration
```sql
-- Just changed maxpagesize or delta_path
UPDATE sap_cdc_registry
SET 
    maxpagesize = 50000,
    delta_path = '/new/path',
    trigger_run_now = TRUE  -- Test it immediately
WHERE job_code = 'TEST_JOB';
```

## Summary

✅ **Use `trigger_run_now = TRUE` to run jobs immediately**
✅ **Works when next_run is NULL or in the future**
✅ **Auto-resets after each run**
✅ **One-time trigger (not persistent)**
✅ **Job must be active (active = TRUE)**

### Quick Command
```sql
-- Run any job immediately
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'YOUR_JOB_CODE';
```

Then wait for the next DAG schedule (default: every 2 minutes) and your job will run! 🚀
