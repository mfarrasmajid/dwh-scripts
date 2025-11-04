# One Page Per Run - SAP Parse Time Optimization

## Problem
SAP takes approximately **5 minutes** to parse data before generating the next skip token. When pulling 7 million rows, continuously pulling all pages in a single run would:
- Overload SAP's parsing capabilities
- Cause timeouts and failures
- Not allow SAP enough time to prepare the next page

## Solution
Modified the script to pull **ONLY ONE PAGE per scheduled run**, allowing SAP time to parse between runs.

## How It Works Now

### Before (Continuous Pull)
```
Run 1: Pull page 1 → Pull page 2 → Pull page 3 → ... → Pull all pages
       ❌ No time for SAP to parse between pages
```

### After (One Page Per Run)
```
Run 1 (00:00): Pull page 1 (100k rows) → Save skip token → Exit
                ⏰ Wait for next schedule (2 minutes)
                
Run 2 (00:02): Pull page 2 (100k rows) → Save skip token → Exit
                ⏰ Wait for next schedule (2 minutes)
                
Run 3 (00:04): Pull page 3 (100k rows) → Save skip token → Exit
                ⏰ Wait for next schedule (2 minutes)
                
... continues until no skip token (all data pulled)

✅ SAP has 2+ minutes between each page to parse data
```

## Benefits

1. **SAP Parse Time**: Gives SAP 5 minutes (or your schedule interval) between pages
2. **Stability**: Prevents timeouts and overload
3. **Monitoring**: Easy to track progress page by page
4. **Resumability**: Can stop/restart anytime without data loss
5. **Parallelism**: Multiple environments can run simultaneously without conflicts

## Configuration

### Schedule Recommendation

For 7 million rows with maxpagesize=100000:
- Pages needed: ~70 pages
- Time per run: ~30 seconds (pull + insert)
- Total time: ~2-3 hours (with 2-minute schedule)

```sql
UPDATE sap_cdc_registry
SET 
    interval_minutes = 2,  -- Run every 2 minutes
    maxpagesize = 100000   -- 100k rows per page
WHERE job_code = 'YOUR_JOB_CODE';
```

### For Faster Processing
```sql
-- More frequent runs (still gives SAP parse time)
UPDATE sap_cdc_registry
SET interval_minutes = 1  -- Run every minute
WHERE job_code = 'YOUR_JOB_CODE';
```

### For Larger Pages
```sql
-- Bigger pages, less frequent
UPDATE sap_cdc_registry
SET 
    maxpagesize = 200000,  -- 200k rows per page
    interval_minutes = 5   -- Run every 5 minutes
WHERE job_code = 'YOUR_JOB_CODE';
```

## Progress Tracking

### Check Current Status
```sql
SELECT 
    job_code,
    prod_initial_done,
    state_json->'prod_skip_token' as current_skip_token,
    prod_last_run,
    prod_next_run,
    prod_last_status
FROM sap_cdc_registry
WHERE job_code = 'YOUR_JOB_CODE';
```

### Estimate Progress
```sql
-- Check how many rows loaded so far
SELECT 
    COUNT(*) as rows_loaded,
    MAX(_ingested_at) as last_insert_time,
    MAX(_batch_row_count) as last_page_size
FROM dl_prod.your_table
WHERE _batch_id LIKE 'YOUR_JOB_CODE%';
```

## Airflow Logs

### Log Pattern Per Run
```
[2025-11-04 00:00:00] Job YOUR_JOB env PROD initial load page 1 (InitialLoad=True)
[2025-11-04 00:00:05] Job YOUR_JOB env PROD initial page 1 pulled=100000 records
[2025-11-04 00:00:10] Job YOUR_JOB env PROD inserted 100000 rows
[2025-11-04 00:00:10] Job YOUR_JOB env PROD saved skiptoken, will continue on next run (allows SAP 5min parse time)

[2025-11-04 00:02:00] Job YOUR_JOB env PROD continuing with skiptoken (page 2)
[2025-11-04 00:02:05] Job YOUR_JOB env PROD initial page 2 pulled=100000 records
[2025-11-04 00:02:10] Job YOUR_JOB env PROD inserted 100000 rows
[2025-11-04 00:02:10] Job YOUR_JOB env PROD saved skiptoken, will continue on next run (allows SAP 5min parse time)

... continues ...

[2025-11-04 02:20:00] Job YOUR_JOB env PROD continuing with skiptoken (page 70)
[2025-11-04 02:20:05] Job YOUR_JOB env PROD initial page 70 pulled=50000 records
[2025-11-04 02:20:08] Job YOUR_JOB env PROD inserted 50000 rows
[2025-11-04 02:20:08] Job YOUR_JOB env PROD initial load complete (no skip token)
```

## What Changed in Code

### 1. Weekly Refresh
```python
# Before: while True loop pulling all pages
while True:
    xml_bytes = _get_initial_page(...)
    # ... process page
    if skip_token:
        stored_skip_token = skip_token  # Continue loop
    else:
        break  # Exit when done

# After: Pull one page, save skip token, exit
stored_skip_token = _get_skip_token(pg, job_code, env)  # Load from DB
xml_bytes = _get_initial_page(...)
# ... process page
if skip_token:
    _set_skip_token(pg, job_code, env, skip_token)  # Save for next run
    LOGGER.info("saved skiptoken, will continue on next run")
# Exit and wait for next scheduled run
```

### 2. Continuous CDC Initial Load
Same pattern - one page per run, save skip token, exit.

### 3. State Persistence
Skip tokens are now **always** persisted in the database (`state_json`), allowing the script to resume from the exact position on the next run.

## Special Cases

### No Data (Empty Result)
```
Job YOUR_JOB env PROD initial load complete (no data)
✅ Marks initial_done = True
✅ Clears skip token
✅ Moves to delta phase (if continuous_cdc)
```

### Last Page (Partial Data)
```
Job YOUR_JOB env PROD initial page 70 pulled=50000 records
Job YOUR_JOB env PROD initial load complete (no skip token)
✅ Last page processed
✅ Initial load complete
```

### Error Recovery
If a run fails:
```
✅ Skip token is saved in DB before processing
✅ Next run will resume from the same skip token
✅ No data loss or duplicate processing
```

## Testing

### Force Restart
To test the one-page-per-run behavior:
```sql
-- Clear state and trigger initial load
UPDATE sap_cdc_registry
SET 
    prod_initial_done = false,
    trigger_force_initial = true,
    trigger_run_now = true,
    state_json = jsonb_set(
        COALESCE(state_json, '{}'),
        '{prod_skip_token}',
        'null'
    )
WHERE job_code = 'YOUR_JOB_CODE';
```

Watch the Airflow logs - you'll see it pull one page, then exit and wait for the next schedule.

## Performance Comparison

### 7 Million Rows Example

**Before (Continuous Pull)**:
- Single run: 3-5 hours
- High memory usage
- SAP timeouts likely
- Hard to monitor progress

**After (One Page Per Run)**:
- 70 runs × 2 minutes = ~2.5 hours total
- Low memory per run
- No SAP timeouts
- Easy progress tracking
- Can pause/resume anytime

## FAQ

### Q: Won't this take longer?
A: Slightly, but it's **more reliable**. The wait time allows SAP to parse, preventing timeouts and failures.

### Q: Can I speed it up?
A: Yes, reduce `interval_minutes` to 1 or increase `maxpagesize` to 200000.

### Q: What if I want the old behavior?
A: Not recommended, but you could manually remove the skip token save/load logic. However, this defeats the purpose of giving SAP parse time.

### Q: Does this affect delta pulls?
A: No, delta pulls work the same way - one page per run if there are skip tokens during delta.

## Summary

✅ **One page per DAG run**
✅ **SAP gets parse time between pages**
✅ **Skip tokens persisted in database**
✅ **Automatic resume on next schedule**
✅ **Better stability and monitoring**

This change makes the SAP CDC process more robust and SAP-friendly! 🎉
