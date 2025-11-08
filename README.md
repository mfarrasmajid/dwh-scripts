# Mitratel Data Warehouse Scripts

Automated data pipeline scripts for Mitratel's Data Warehouse infrastructure using Apache Airflow, Debezium, Kafka, ClickHouse, and PostgreSQL.

## 📋 Table of Contents

- [Architecture](#architecture)
- [Directory Structure](#directory-structure)
- [Quick Links](#quick-links)
- [Infrastructure](#infrastructure)
- [DAG Categories](#dag-categories)
- [SAP CDC Integration](#sap-cdc-integration)
- [Getting Started](#getting-started)
- [Documentation](#documentation)

## 🏗️ Architecture

```
Data Sources (SAP, Databases) 
    ↓
Apache Airflow DAGs (Orchestration)
    ↓
Data Lake (ClickHouse + PostgreSQL)
    ↓
Data Warehouse (ClickHouse)
    ↓
Data Mart (PostgreSQL)
    ↓
Dashboard & Analytics
```

## 📁 Directory Structure

```
scripts/
├── DL/          # Data Lake ingestion scripts
│   ├── SAP/     # SAP OData CDC jobs
│   └── bak/     # Legacy/archived scripts
├── DM/          # Data Mart transformation scripts
├── DWH/         # Data Warehouse processing scripts
└── helper/      # Utility scripts and helpers
```

## 🔗 Quick Links

| Service | URL | Description |
|---------|-----|-------------|
| **ClickHouse UI** | [http://dwhapps.mitratel.co.id:5521](http://dwhapps.mitratel.co.id:5521) | Database web interface |
| **Apache Airflow** | [http://dwhapps.mitratel.co.id:8080/home](http://dwhapps.mitratel.co.id:8080/home) | DAG orchestration UI |
| **DWH Monitoring** | [https://dwhapps.mitratel.co.id/](https://dwhapps.mitratel.co.id/) | System monitoring dashboard |
| **Analytics Dashboard** | [https://dashboard.mitratel.co.id](https://dashboard.mitratel.co.id) | Business intelligence dashboards |

## 🖥️ Infrastructure

| Component | IP Address | Purpose |
|-----------|------------|---------|
| **Data Lake** | 192.168.101.158 | ClickHouse + PostgreSQL (raw data) |
| **Data Warehouse** | 192.168.101.159 | ClickHouse (processed data) |
| **Data Mart** | 192.168.101.165 | PostgreSQL (aggregated data) |
| **Logging** | 192.168.101.158 | PostgreSQL (application logs) |
| **Kafka** | Background Service | Message broker for CDC |
| **Debezium** | Background Service | Database CDC connector |

## 📊 DAG Categories

### Data Lake (DL)
**Purpose**: Extract and load raw data from source systems

- `DL_sap_all_table.py` - SAP OData CDC with delta queries
- `DL_exware_generate_*.py` - Exware data ingestion
- `DL_oneflux_all_table_keyset.py` - OneFlux data extraction
- `DL_supof_generate_*.py` - Supof data processing

**Key Features**:
- Change Data Capture (CDC) support
- Skip token pagination for large datasets
- Delta token management
- Automatic schema synchronization
- WIB timezone handling

### Data Warehouse (DWH)
**Purpose**: Transform and structure data for analytics

- `DWH_all_table.py` - Data warehouse transformations

### Data Mart (DM)
**Purpose**: Create aggregated views for reporting

- `DM_all_table.py` - Data mart aggregations

## 🔄 SAP CDC Integration

### Overview
The SAP CDC system uses OData v4 delta queries to efficiently sync data changes from SAP systems.

### Key Features
- ✅ **Delta Queries**: Only fetch changed records using delta tokens
- ✅ **Skip Token Pagination**: Handle millions of rows efficiently
- ✅ **One Page Per Run**: Respects SAP's 5-minute parse time between pages
- ✅ **Automatic Recovery**: Resumes from last position on failure
- ✅ **Dual Mode**: Supports both `continuous_cdc` and `weekly_refresh`
- ✅ **Timezone Support**: All timestamps in WIB (GMT+7)

### Configuration
Jobs are configured in the `sap_cdc_registry` table:

```sql
-- Example job configuration
INSERT INTO sap_cdc_registry (
    job_code, initial_path, delta_path, entity_name,
    cdc_method, interval_minutes, maxpagesize,
    prod_enabled, prod_db, prod_table, prod_client
) VALUES (
    'AFIH', 
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH',
    '/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH',
    'ZCDC_AFIH',
    'continuous_cdc', 
    5, 
    100000,
    TRUE, 
    'dl_prod', 
    'sap_afih', 
    '300'
);
```

### Manual Triggers
Force immediate execution:
```sql
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'AFIH';
```

Force initial load (truncate and reload):
```sql
UPDATE sap_cdc_registry
SET trigger_force_initial = TRUE, trigger_run_now = TRUE
WHERE job_code = 'AFIH';
```

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Apache Airflow 2.x
- Access to Mitratel VPN
- Database credentials configured in Airflow Connections

### Installation
```bash
# Clone repository
cd /path/to/airflow/dags

# Install dependencies (if needed)
pip install -r requirements.txt
```

### Running a DAG
1. Access Airflow UI: http://dwhapps.mitratel.co.id:8080
2. Navigate to the desired DAG
3. Enable the DAG using the toggle switch
4. Click "Trigger DAG" to run manually

### Monitoring
- **Airflow Logs**: Check task logs in Airflow UI for detailed execution info
- **Database Logs**: Query `public.airflow_logs` table for job history
- **Monitoring Dashboard**: View real-time metrics at monitoring portal

## 📚 Documentation

Detailed documentation is available in each directory:

- [SAP CDC Changes](DL/SAP_CDC_CHANGES.md) - Complete SAP CDC implementation guide
- [Delta Path Setup](DL/DELTA_PATH_SETUP_GUIDE.md) - Troubleshooting delta URLs
- [One Page Per Run](DL/ONE_PAGE_PER_RUN.md) - SAP pagination optimization
- [Trigger Run Now](DL/TRIGGER_RUN_NOW.md) - Manual job execution guide
- [Initial Top Removal](DL/INITIAL_TOP_REMOVAL.md) - Migration to skip tokens

### Key Concepts

**CDC (Change Data Capture)**
- Captures only changed records instead of full table scans
- Uses delta tokens to track changes
- Significantly reduces load on source systems

**Skip Tokens**
- Server-side pagination mechanism
- SAP generates tokens for fetching next page
- Replaces traditional $skip/$top parameters

**Delta Tokens**
- Identifies point-in-time snapshot
- Retrieved from DeltaLinksOf endpoint
- Used to fetch changes since last sync

**WIB Timezone**
- All timestamps stored in WIB (GMT+7)
- Matches local business hours
- Simplifies scheduling and monitoring

## 🛠️ Troubleshooting

### Common Issues

**Job not running**
```sql
-- Check next_run time
SELECT job_code, prod_next_run, prod_last_status, prod_last_error
FROM sap_cdc_registry
WHERE job_code = 'YOUR_JOB';

-- Trigger immediate run
UPDATE sap_cdc_registry
SET trigger_run_now = TRUE
WHERE job_code = 'YOUR_JOB';
```

**404 on DeltaLinksOf endpoint**
- Check `delta_path` column in registry
- Verify endpoint exists in SAP
- See [Delta Path Setup Guide](DL/DELTA_PATH_SETUP_GUIDE.md)

**Large dataset timeout**
- Increase `maxpagesize` (default: 100000)
- System pulls one page per run (prevents SAP overload)
- See [One Page Per Run](DL/ONE_PAGE_PER_RUN.md)

### Support
For issues or questions, contact the Data Warehouse team or check the detailed documentation in each subdirectory.

## 📝 License

Internal use only - Mitratel Data Warehouse Team

---

**Last Updated**: November 2025  
**Maintained By**: Data Engineering Team  
**Version**: 2.0