from __future__ import annotations

import logging
import time
import requests
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---- Python logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("DL_sap_custom_acdoca")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)

# ===== DAG meta =====
DAG_ID = "DL_sap_custom_acdoca"
DEFAULT_SCHEDULE = "0 */3 * * *"  # Every 3 hours
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ===== Database/Table config =====
TARGET_DB = "sap_custom"
TARGET_TABLE = "acdoca"
SOURCE_DB = "sap_prod"
SOURCE_TABLE = "zdmt_fitb040"

# ===== Logging config =====
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "public.airflow_logs"
LOG_TYPE = "left join fetch"
LOG_KATEGORI = "Data Lake"

# ===== SAP PO config =====
SAP_PO_URL = "http://dmtpoprd.mitratel.co.id:53100/RESTAdapter/OneFlux/ReadTable"
SAP_USERNAME_VAR = "SAP_PO_AUTH_USER"
SAP_PASSWORD_VAR = "SAP_PO_AUTH_PASS"

# ===== ClickHouse connection =====
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"


def _json_safe(v):
    """Convert values to JSON-safe types"""
    if v is None:
        return None
    return str(v).strip() if str(v).strip() else None


def _log_status(log_hook, process_name: str, mark: str, status: str,
                ck_db: str, ck_table: str, error_message: Optional[str] = None):
    """Log DAG execution status to Postgres"""
    dag_name = f"{ck_db}.{ck_table}"
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = log_hook.get_conn()
    cur = conn.cursor()
    try:
        if status == "pending":
            cur.execute(
                f"INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori) "
                f"VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
            )
        else:
            cur.execute(
                f"UPDATE {LOG_TABLE} SET status=%s, end_time=%s, error_message=%s "
                f"WHERE process_name=%s AND status='pending' AND dag_name=%s AND mark=%s AND kategori=%s",
                (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
            )
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@contextmanager
def phase_logger(log_hook, process_name: str, mark: str, ck_db: str, ck_table: str):
    """Context manager for logging phases"""
    _log_status(log_hook, process_name, mark, "pending", ck_db, ck_table)
    try:
        yield
        _log_status(log_hook, process_name, mark, "success", ck_db, ck_table)
    except Exception as e:
        _log_status(log_hook, process_name, mark, "error", ck_db, ck_table, error_message=str(e))
        raise


def ck_exec(ch: ClickHouseHook, sql: str, params=None):
    """Execute ClickHouse query (compatibility helper)"""
    if hasattr(ch, "execute"):
        try:
            return ch.execute(sql, params) if params is not None else ch.execute(sql)
        except TypeError:
            return ch.execute(sql)
    if hasattr(ch, "run"):
        try:
            return ch.run(sql, parameters=params) if params is not None else ch.run(sql)
        except TypeError:
            return ch.run(sql)
    conn = ch.get_conn()
    return conn.execute(sql, params) if params is not None else conn.execute(sql)


def ck_query(ch: ClickHouseHook, sql: str) -> List[Tuple]:
    """Query ClickHouse and return results"""
    if hasattr(ch, "get_pandas_df"):
        df = ch.get_pandas_df(sql)
        return [tuple(row) for row in df.itertuples(index=False, name=None)]
    conn = ch.get_conn()
    return list(conn.execute(sql))


def ck_insert(ch: ClickHouseHook, sql: str, data: List[Dict]):
    """Insert data into ClickHouse"""
    if not data:
        return
    if hasattr(ch, "execute"):
        try:
            return ch.execute(sql, data)
        except TypeError:
            return ch.execute(sql)
    if hasattr(ch, "run"):
        try:
            return ch.run(sql, parameters=data)
        except TypeError:
            return ch.run(sql)
    conn = ch.get_conn()
    return conn.execute(sql, data)


# ===== DAG =====
with DAG(
    dag_id=DAG_ID,
    schedule=DEFAULT_SCHEDULE,
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["SAP", "ACDOCA", "ClickHouse"],
) as dag:

    @task()
    def setup_table():
        """Create database and table if not exist"""
        mark = f"setup_{int(time.time())}"
        log_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
        ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        
        with phase_logger(log_hook, f"{DAG_ID}:setup", mark, TARGET_DB, TARGET_TABLE):
            # Create database
            ck_exec(ch, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
            LOGGER.info(f"Database {TARGET_DB} ensured")
            
            # Create table with ReplacingMergeTree engine
            # Primary key is DOCLN
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
                BELNR String,
                GJAHR String,
                AUFNR String,
                DOCLN String,
                _updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(_updated_at)
            ORDER BY (BELNR, GJAHR, AUFNR, DOCLN)
            PARTITION BY toYYYYMM(_updated_at)
            SETTINGS index_granularity = 8192
            """
            ck_exec(ch, create_sql)
            LOGGER.info(f"Table {TARGET_DB}.{TARGET_TABLE} ensured")
        
        return mark

    @task()
    def query_missing_aufnr(mark: str) -> List[Dict[str, Any]]:
        """Query ZDMT_FITB040 to find records where AUFNR is null in acdoca"""
        log_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
        ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        
        with phase_logger(log_hook, f"{DAG_ID}:query_missing", mark, TARGET_DB, TARGET_TABLE):
            # Query to find records where AUFNR needs to be fetched
            # Only select records where no matching record exists in acdoca (left join is NULL)
            query_sql = f"""
            SELECT DISTINCT
                z.INVACCDOC as BELNR,
                z.INVACCYEAR as GJAHR
            FROM {SOURCE_DB}.{SOURCE_TABLE} z
            LEFT JOIN {TARGET_DB}.{TARGET_TABLE} a 
                ON z.INVACCDOC = a.BELNR 
                AND z.INVACCYEAR = a.GJAHR
            WHERE z.PRNO IS NULL
            LIMIT 10000
            """
            
            results = ck_query(ch, query_sql)
            
            records = []
            for row in results:
                belnr = _json_safe(row[0])
                gjahr = _json_safe(row[1])
                
                if belnr and gjahr:
                    records.append({
                        "BELNR": belnr,
                        "GJAHR": gjahr
                    })
            
            LOGGER.info(f"Found {len(records)} records with missing AUFNR")
            
            # Log the count
            _log_status(log_hook, f"{DAG_ID}:query_missing", mark, "success", 
                       TARGET_DB, TARGET_TABLE, 
                       error_message=f"Found {len(records)} records to process")
        
        return records

    @task()
    def fetch_aufnr_from_sap(records: List[Dict[str, Any]], mark: str) -> List[Dict[str, Any]]:
        """Call SAP PO REST API once to fetch AUFNR for all records"""
        if not records:
            LOGGER.info("No records to process")
            return []
        
        log_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
        
        # Get SAP credentials from Airflow Variables
        try:
            sap_username = Variable.get(SAP_USERNAME_VAR)
            sap_password = Variable.get(SAP_PASSWORD_VAR)
        except Exception as e:
            LOGGER.error(f"Failed to get SAP credentials: {e}")
            raise
        
        results = []
        
        with phase_logger(log_hook, f"{DAG_ID}:fetch_sap", mark, TARGET_DB, TARGET_TABLE):
            LOGGER.info(f"Fetching AUFNR for {len(records)} records in single SAP PO call")
            
            # Build Options array with all BELNR and GJAHR combinations
            options = []
            for i, record in enumerate(records):
                belnr = record["BELNR"]
                gjahr = record["GJAHR"]
                
                if i == 0:
                    # First record: (BELNR = 'xxx' AND
                    options.append({"Text": f"(BELNR = '{belnr}' AND "})
                    # GJAHR = 'yyy')
                    if len(records) == 1:
                        options.append({"Text": f"GJAHR = '{gjahr}')"})
                    else:
                        options.append({"Text": f"GJAHR = '{gjahr}') OR "})
                elif i == len(records) - 1:
                    # Last record: (BELNR = 'xxx' AND
                    options.append({"Text": f"(BELNR = '{belnr}' AND "})
                    # GJAHR = 'yyy')
                    options.append({"Text": f"GJAHR = '{gjahr}')"})
                else:
                    # Middle records: (BELNR = 'xxx' AND
                    options.append({"Text": f"(BELNR = '{belnr}' AND "})
                    # GJAHR = 'yyy') OR
                    options.append({"Text": f"GJAHR = '{gjahr}') OR "})
            
            # Construct request payload with AUFNR, BELNR, GJAHR, and DOCLN fields
            payload = {
                "Table": "ACDOCA",
                "Delimiter": "|",
                "NoData": "",
                "RowSkips": "",
                "RowCount": "",
                "Options": options,
                "Fields": [
                    {"Name": "AUFNR"},
                    {"Name": "BELNR"},
                    {"Name": "GJAHR"},
                    {"Name": "DOCLN"}
                ]
            }
            
            try:
                # Call SAP PO REST API once
                LOGGER.info(f"Calling SAP PO with {len(options)} option texts")
                response = requests.post(
                    SAP_PO_URL,
                    json=payload,
                    auth=(sap_username, sap_password),
                    timeout=120
                )
                response.raise_for_status()
                
                # Parse response
                sap_response = response.json()
                data_lines = sap_response.get("Data", [])
                
                LOGGER.info(f"Received {len(data_lines)} data lines from SAP PO")
                
                # Parse delimited lines: AUFNR|BELNR|GJAHR|DOCLN
                for line_obj in data_lines:
                    line_val = line_obj.get("Line", "")
                    if not line_val:
                        continue
                    
                    # Split by delimiter and strip whitespace
                    parts = [p.strip() for p in str(line_val).split("|")]
                    
                    if len(parts) >= 3:
                        aufnr = parts[0] if parts[0] else ''
                        belnr = parts[1] if parts[1] else ''
                        gjahr = parts[2] if parts[2] else ''
                        docln = parts[3] if len(parts) >= 4 and parts[3] else ''
                        
                        # Only add if BELNR and GJAHR are present
                        if belnr and gjahr:
                            results.append({
                                "BELNR": belnr,
                                "GJAHR": gjahr,
                                "AUFNR": aufnr,  # Can be empty string
                                "DOCLN": docln   # Can be empty string
                            })
                            
                            # if aufnr:
                                # LOGGER.info(f"  ✓ BELNR={belnr}, GJAHR={gjahr}, AUFNR={aufnr}")
                            # else:
                                # LOGGER.info(f"  ✓ BELNR={belnr}, GJAHR={gjahr}, AUFNR=(empty)")
                
                # Log summary
                msg = f"Retrieved {len(results)} records from SAP PO"
                LOGGER.info(msg)
                _log_status(log_hook, f"{DAG_ID}:fetch_sap", mark, "success",
                           TARGET_DB, TARGET_TABLE, error_message=msg)
                
            except requests.exceptions.RequestException as e:
                LOGGER.error(f"SAP API error: {e}")
                _log_status(log_hook, f"{DAG_ID}:fetch_sap", mark, "error",
                           TARGET_DB, TARGET_TABLE, error_message=str(e))
                raise
            except Exception as e:
                LOGGER.error(f"Unexpected error: {e}")
                _log_status(log_hook, f"{DAG_ID}:fetch_sap", mark, "error",
                           TARGET_DB, TARGET_TABLE, error_message=str(e))
                raise
        
        return results

    @task()
    def insert_clickhouse(results: List[Dict[str, Any]], mark: str):
        """Insert results into ClickHouse acdoca table (ReplacingMergeTree handles deduplication)"""
        if not results:
            LOGGER.info("No results to insert")
            return
        
        log_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
        ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        
        with phase_logger(log_hook, f"{DAG_ID}:insert_ch", mark, TARGET_DB, TARGET_TABLE):
            # Prepare data for insert (exclude records where AUFNR is empty)
            insert_data = []
            filtered_count = 0
            for result in results:
                aufnr = result.get("AUFNR", '')
                # Skip records where AUFNR is empty string
                if aufnr == '':
                    filtered_count += 1
                    continue
                    
                insert_data.append({
                    "BELNR": result["BELNR"],
                    "GJAHR": result["GJAHR"],
                    "AUFNR": aufnr,
                    "DOCLN": result.get("DOCLN", ''),
                    "_updated_at": datetime.now()
                })
            
            if filtered_count > 0:
                LOGGER.info(f"Filtered out {filtered_count} records with empty AUFNR")
            
            # Insert into ClickHouse (ReplacingMergeTree automatically replaces old records by BELNR, GJAHR)
            insert_sql = f"""
            INSERT INTO {TARGET_DB}.{TARGET_TABLE} 
            (BELNR, GJAHR, AUFNR, DOCLN, _updated_at) 
            VALUES
            """
            
            ck_insert(ch, insert_sql, insert_data)
            
            LOGGER.info(f"Inserted {len(insert_data)} records into {TARGET_DB}.{TARGET_TABLE}")
            
            # Log success with details
            msg = f"Inserted {len(insert_data)} records (filtered {filtered_count} records with empty AUFNR)"
            _log_status(log_hook, f"{DAG_ID}:insert_ch", mark, "success",
                       TARGET_DB, TARGET_TABLE, error_message=msg)

    # Define task dependencies
    mark = setup_table()
    records = query_missing_aufnr(mark)
    results = fetch_aufnr_from_sap(records, mark)
    insert_clickhouse(results, mark)
