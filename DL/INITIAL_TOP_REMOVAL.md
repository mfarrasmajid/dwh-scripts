# Removal of initial_top Parameter

## Summary
Removed `initial_top` parameter from the SAP CDC implementation since we no longer use `$skip` and `$top` pagination.

## What Changed

### Before (Old Implementation)
- Used `$skip` and `$top` parameters for pagination
- URL: `FactsOfZCDCAFIH?sap-client=300&$skip=0&$top=5000`
- Required `initial_top` column in registry

### After (New Implementation)
- Uses `InitialLoad=true` parameter for first request
- Uses skip tokens from `<link rel='next'>` for pagination
- URL: `FactsOfZCDCAFIH?sap-client=300&InitialLoad=true`
- Then: `FactsOfZCDCAFIH?sap-client=300&$skiptoken=XXX`
- **No longer needs `initial_top`**

## Code Changes

### 1. Removed Variable
```python
# REMOVED
top = int(job.get("initial_top") or 5000)
```

### 2. Updated Function Signature
```python
# Before
def _get_initial_page(session, url_base, skip, top, auth, maxpagesize, use_initial_load):
    if use_initial_load:
        url = f"{url_base}?InitialLoad=true"
    else:
        url = f"{url_base}?$skip={skip}&$top={top}"
    
# After
def _get_initial_page(session, url_base, auth, maxpagesize, use_initial_load):
    if use_initial_load:
        url = f"{url_base}?InitialLoad=true"
    else:
        url = url_base  # Fallback without InitialLoad
```

### 3. Updated Function Calls
```python
# Before
xml_bytes = _get_initial_page(session, initial_url, 0, top, auth, maxpagesize, use_initial_load)

# After
xml_bytes = _get_initial_page(session, initial_url, auth, maxpagesize, use_initial_load)
```

## Database Cleanup (Optional)

You can optionally remove the `initial_top` column from your registry table:

```sql
-- Optional: Remove unused column
ALTER TABLE sap_cdc_registry DROP COLUMN IF EXISTS initial_top;

-- Verify it's gone
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'sap_cdc_registry' 
  AND column_name = 'initial_top';
```

## Why This Change?

1. **Simpler Logic**: No need to track skip/top counters
2. **SAP Recommended**: `InitialLoad=true` is the proper CDC approach
3. **More Efficient**: Skip tokens are managed by SAP, optimized internally
4. **Less Configuration**: One less parameter to maintain

## Impact

✅ **No Breaking Changes**: The code still works if `initial_top` exists in the database
✅ **Cleaner Code**: Removed unused parameter handling
✅ **Better Performance**: Skip tokens are more efficient than $skip/$top

## Testing

The implementation has been updated and tested:
- ✅ Initial load with `InitialLoad=true`
- ✅ Skip token extraction and continuation
- ✅ Delta token retrieval
- ✅ Weekly refresh mode

All functionality works without `initial_top`.
