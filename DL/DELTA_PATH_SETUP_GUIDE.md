# Delta Path Setup Guide for SAP CDC

## Critical Fix: Use `delta_path` Column

The system now **prioritizes** the `delta_path` column from the registry table for fetching delta tokens. This fixes the 404 error you encountered.

## Problem Example

**Error**:
```
404 Client Error: Not found for url: 
http://dmterpprd2.mitratel.co.id:8031/DeltaLinksOfFactsOfZCDCZDMTMMTB020?sap-client=300
```

**Root Cause**: Missing `/sap/opu/odata/sap/SERVICE_NAME/` in the path.

## Solution

Always set **both** `initial_path` and `delta_path` with **full paths**:

```sql
UPDATE sap_cdc_registry
SET 
    initial_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020',
    delta_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020',
    maxpagesize = 100000
WHERE job_code = 'YOUR_JOB_CODE';
```

## Path Pattern

### Template
```
initial_path: /sap/opu/odata/sap/{SERVICE_NAME}/{FACTS_ENTITY}
delta_path:   /sap/opu/odata/sap/{SERVICE_NAME}/DeltaLinksOf{FACTS_ENTITY}
```

### Real Examples

#### Example 1: AFIH
```sql
initial_path = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH'
delta_path   = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH'
```

#### Example 2: FMZUOB
```sql
initial_path = '/sap/opu/odata/sap/ZCDC_FMZUOB_1_SRV/FactsOfZCDCFMZUOB'
delta_path   = '/sap/opu/odata/sap/ZCDC_FMZUOB_1_SRV/DeltaLinksOfFactsOfZCDCFMZUOB'
```

#### Example 3: ZDMT_MMTB020
```sql
initial_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020'
delta_path   = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020'
```

## How the Code Works

```python
# From DL_sap_all_table.py line ~925
if delta_path and delta_path != initial_path:
    # Use the explicit delta_path from registry
    delta_links_url = _compose_url(base_url, delta_path, client)
else:
    # Fallback: try to construct from entity_name (less reliable)
    delta_links_path = _get_delta_links_path(entity_name)
    delta_links_url = _compose_url(base_url, delta_links_path, client)
```

## Migration Script for All Jobs

```sql
-- Update all jobs with proper delta_path
-- Adjust the patterns based on your actual service names

-- Pattern 1: Service ends with _SRV, entity starts with FactsOf
UPDATE sap_cdc_registry
SET delta_path = REPLACE(initial_path, 'FactsOf', 'DeltaLinksOfFactsOf')
WHERE delta_path IS NULL 
  AND initial_path LIKE '%/FactsOf%'
  AND service_name LIKE '%_SRV';

-- Verify the update
SELECT 
    job_code,
    service_name,
    initial_path,
    delta_path,
    CASE 
        WHEN delta_path IS NULL THEN '❌ MISSING'
        WHEN delta_path = initial_path THEN '⚠️  SAME AS INITIAL'
        ELSE '✅ OK'
    END as status
FROM sap_cdc_registry
WHERE is_enabled = true
ORDER BY status DESC, job_code;
```

## Testing Delta Path

You can test if your delta_path is correct using curl:

```bash
# Replace with your values
BASE_URL="http://dmterpprd2.mitratel.co.id:8031"
DELTA_PATH="/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH"
SAP_CLIENT="300"
USERNAME="your_user"
PASSWORD="your_pass"

curl -u "$USERNAME:$PASSWORD" \
  "${BASE_URL}${DELTA_PATH}?sap-client=${SAP_CLIENT}"
```

Expected response should be XML with `<d:DeltaToken>` element:
```xml
<feed xmlns="http://www.w3.org/2005/Atom">
    <entry>
        <content type="application/xml">
            <m:properties>
                <d:DeltaToken>D20251104043043_000037000</d:DeltaToken>
                ...
            </m:properties>
        </content>
    </entry>
</feed>
```

## Common Mistakes

### ❌ Wrong: Missing service path
```sql
delta_path = '/DeltaLinksOfFactsOfZCDCAFIH'
```

### ❌ Wrong: Using relative path
```sql
delta_path = 'DeltaLinksOfFactsOfZCDCAFIH'
```

### ❌ Wrong: Same as initial_path
```sql
initial_path = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH'
delta_path   = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH'
```

### ✅ Correct: Full absolute path with DeltaLinksOf
```sql
initial_path = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH'
delta_path   = '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH'
```

## Environment URL Construction

The final URL is built as:
```
{base_url from sap_cdc_env_prefix} + {delta_path} + ?sap-client={client}
```

Example:
- base_url: `http://dmterpprd2.mitratel.co.id:8031`
- delta_path: `/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH`
- client: `300`
- **Final URL**: `http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH?sap-client=300`

## Troubleshooting

### Issue: Still getting 404 errors

**Check**:
1. Is `delta_path` column populated?
   ```sql
   SELECT job_code, delta_path FROM sap_cdc_registry WHERE job_code = 'YOUR_JOB';
   ```

2. Is the path correct in SAP?
   - Log into SAP Gateway
   - Navigate to `/IWFND/MAINT_SERVICE`
   - Find your service and verify the EntitySet names

3. Check the Airflow logs for the actual URL being called:
   ```
   [DEBUG] GET http://... | status=404
   ```

### Issue: Delta token not found in response

**Check**:
1. Is this the first run? Initial load must complete first.
2. Has 10 minutes passed since initial load completed?
3. Check if the service supports delta queries (some SAP services don't).

## Quick Fix for Current Error

Based on your error, run this immediately:

```sql
UPDATE sap_cdc_registry
SET 
    initial_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020',
    delta_path = '/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020',
    maxpagesize = 100000,
    trigger_run_now = true
WHERE service_name = 'ZCDC_ZDMT_MMTB020_2_SRV';
```

Then check the next DAG run in Airflow.
