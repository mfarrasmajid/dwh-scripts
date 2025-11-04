# SAP CDC Enhancement - Implementation Summary

## Overview
Enhanced the SAP CDC implementation to properly handle SAP OData CDC with `InitialLoad`, skip tokens, and delta tokens according to SAP best practices.

## Key Changes

### 1. **Initial Load with InitialLoad=true**
- **Before**: Used `$skip` and `$top` parameters for pagination
- **After**: Uses `InitialLoad=true` parameter for first request
- **Headers**: `Accept-Encoding: gzip` and `Prefer: odata.track-changes,odata.maxpagesize={value}`
- **URL Pattern**: `http://.../FactsOfZCDCAFIH?sap-client=300&InitialLoad=true`

### 2. **Skip Token Handling**
- Extracts skip tokens from `<link rel='next'>` in the feed response
- URL format: `FactsOf...?sap-client=300&InitialLoad=true&$skiptoken=XXX`
- Stores skip token in `state_json` for continuation across DAG runs
- Works during both initial load and delta pulls

### 3. **Delta Token Management**
- After initial load completes, fetches delta token from `DeltaLinksOf{EntityName}` endpoint
- URL format: `http://.../DeltaLinksOfFactsOfZCDCAFIH?sap-client=300`
- **No headers** for DeltaLinksOf requests
- Parses `<d:DeltaToken>` from XML response
- Stores in `state_json` as `{env}_delta_token`

### 4. **10-Minute Wait Period**
- Records `initial_completed_at` timestamp when initial load finishes
- Waits 10 minutes before starting delta pulls (allows SAP to complete parsing)
- Checks elapsed time: `(now - initial_completed_at) < 10 minutes`

### 5. **Delta Pull Logic**
Two scenarios:

#### A. **Skip Token Continuation** (if skip token exists)
- URL: `http://.../FactsOfZCDCAFIH?sap-client=300&$skiptoken=YYYY`
- Headers: `Prefer: odata.track-changes,odata.maxpagesize={value}`
- Saves new skip token if returned

#### B. **Delta Token Pull** (no skip token)
- URL: `http://.../FactsOfZCDCAFIH?sap-client=300&$deltatoken=ZZZZ`
- Headers: `Prefer: odata.track-changes,odata.maxpagesize={value}`
- If response has skip token: saves it for next run
- If no skip token: fetches fresh delta token from DeltaLinksOf endpoint

### 6. **Weekly Refresh Mode**
- Truncates table
- Performs full initial load with skip token handling
- Clears all CDC state (delta tokens, skip tokens)

### 7. **Configurable Parameters**
New columns in `sap_cdc_registry`:
- `maxpagesize` (INTEGER, default 100000): Page size for OData requests
- `entity_name` (TEXT): Entity name for DeltaLinksOf path construction (OPTIONAL - only if delta_path not provided)

**Note**: `initial_top` is **no longer used** since we switched from `$skip`/`$top` pagination to skip token-based pagination. You can optionally drop this column.

**Important**: Use `delta_path` column to specify the DeltaLinksOf endpoint path. Example:
- `initial_path`: `/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH`
- `delta_path`: `/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH`

If `delta_path` is not set or same as `initial_path`, the system will try to construct it from `entity_name`.

Stored in `state_json` JSONB:
- `{env}_delta_token`: Current delta token
- `{env}_skip_token`: Current skip token for pagination
- `{env}_initial_completed_at`: Initial load completion timestamp

## Database Migration

Run the migration script:
```sql
-- See migration_add_cdc_columns.sql
ALTER TABLE sap_cdc_registry ADD COLUMN maxpagesize INTEGER DEFAULT 100000;
ALTER TABLE sap_cdc_registry ADD COLUMN entity_name TEXT;
```

## Usage Example

### Registry Configuration
```sql
INSERT INTO sap_cdc_registry (
    job_code, 
    service_name, 
    entity_name,  -- OPTIONAL (only if delta_path not provided)
    method,
    initial_path,
    delta_path,   -- IMPORTANT: Specify DeltaLinksOf path here
    maxpagesize,  -- NEW
    sim_client,
    sim_enabled,
    sim_db,
    sim_table
) VALUES (
    'SAP_AFIH',
    'ZCDC_AFIH_1_SRV',
    'ZCDC_AFIH_1',  -- Only used if delta_path is NULL or same as initial_path
    'continuous_cdc',
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH',
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH',  -- Full path to DeltaLinksOf endpoint
    100000,  -- Configurable page size
    '300',
    true,
    'dl_sim',
    'sap_afih'
);
```

### Flow Diagram

```
Initial Load Phase:
┌─────────────────────────────────────────────┐
│ 1. GET /FactsOf...?InitialLoad=true        │
│    Headers: maxpagesize=100000              │
├─────────────────────────────────────────────┤
│ 2. Parse response, extract skip token      │
│    from <link rel='next'>                   │
├─────────────────────────────────────────────┤
│ 3. If skip token exists:                    │
│    GET /FactsOf...?$skiptoken=XXX           │
│    Repeat until no skip token               │
├─────────────────────────────────────────────┤
│ 4. Initial load complete                    │
│    GET /DeltaLinksOf... (no headers)        │
│    Save delta token                         │
└─────────────────────────────────────────────┘
                    ↓
          Wait 10 minutes
                    ↓
┌─────────────────────────────────────────────┐
│ Delta Pull Phase:                           │
│                                             │
│ Check if skip token exists:                 │
│ ├─ YES: GET /FactsOf...?$skiptoken=YYY     │
│ │       Continue pagination                 │
│ └─ NO:  GET /FactsOf...?$deltatoken=ZZZ    │
│         If no new skip token:               │
│         Fetch new delta token from          │
│         DeltaLinksOf endpoint               │
└─────────────────────────────────────────────┘
```

## Code Changes Summary

### New Functions
- `_make_headers(maxpagesize)`: Dynamic header creation
- `_extract_skiptoken_from_feed()`: Parse skip token from XML
- `_get_delta_token()` / `_set_delta_token()`: Delta token management
- `_get_skip_token()` / `_set_skip_token()`: Skip token management
- `_get_initial_completed_at()` / `_set_initial_completed_at()`: Timestamp tracking
- `_get_delta_links_path()`: Construct DeltaLinksOf endpoint path

### Modified Functions
- `_parse_feed()`: Now returns 5 values (added skip_token)
- `_get_initial_page()`: Added `maxpagesize`, `use_initial_load` parameters
- `_get_url()`: Added `maxpagesize`, `add_headers` parameters
- `process_job()`: Complete rewrite of continuous_cdc logic

## Testing Checklist

- [ ] Initial load with InitialLoad=true parameter
- [ ] Skip token extraction and continuation
- [ ] Delta token retrieval from DeltaLinksOf endpoint
- [ ] 10-minute wait enforcement
- [ ] Delta pull with delta token
- [ ] Skip token handling during delta pull
- [ ] Delta token refresh when no skip token
- [ ] Weekly refresh truncation and reload
- [ ] Force initial trigger clears all state
- [ ] Multiple environments (SIM/QAS/PROD) work independently

## Breaking Changes

⚠️ **Important**: The continuous_cdc logic has been completely rewritten. Existing jobs may need to:
1. Add `maxpagesize` value (defaults to 100000 if not set)
2. Add `entity_name` value for DeltaLinksOf path construction
3. Clear existing state if upgrading mid-run: `UPDATE sap_cdc_registry SET trigger_force_initial=true WHERE job_code='...'`

## References

- SAP OData CDC Documentation
- OData v4 Delta Links specification
- SAP NetWeaver Gateway Delta Query Implementation
