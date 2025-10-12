# dags/dwh_spmk_factconst2_ch2pg.py
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from psycopg2.extras import execute_values

# --------------------
# CONFIG
# --------------------
DAG_ID = "DM_spmk_const2"
SCHEDULE = "9-59/15 * * * *"      # every 15 minutes after minute 9
CATCHUP = False
START_DATE = datetime(2025, 1, 1)

# ClickHouse (SECOND CH = the already built table)
CH_CONN_ID = "clickhouse_dwh_mitratel"
CH_DB = "dwh_spmk"
CH_TABLE = "fact_const2"

# PostgreSQL target
PG_CONN_ID = "db_datamart_mitratel"   # <-- set to your Airflow Postgres connection
PG_SCHEMA = "public"
PG_TABLE = "mart_spmk_const_v2"

# Optional full refresh flag
FORCE_FULL_VAR = "DM_SPMK_FORCE_FULL"

# Airflow log table (same shape as your other DAGs)
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "upsert"
LOG_KATEGORI = "Data Mart"

TAGS = ["dm", "spmk", "clickhouse", "postgres", "c2p"]

# Chunk sizes
FETCH_CHUNK = 50_000       # rows per fetch from ClickHouse
UPSERT_CHUNK = 10_000      # rows per execute_values batch to Postgres

# Columns (order matters; must match CH table and target PG table)
COLUMNS = [
    "project_id","spmk_number","spmk_cancel","spmk_value","regional","spmk_status","spmk_sow",
    "spmk_approved_date","spmk_create_date","boq_number","boq_status","boq_sow","boq_standard_value",
    "boq_addwork_value","boq_value","boq_create_date","boq_approved_date","boq_draft_date",
    "boq_rejected_date","boq_pm_ro_date","boq_manager_codm_ro_date","boq_gm_area_date",
    "boq_manager_construction_project_deployment_date","boq_gm_construction_project_management_date",
    "boq_manager_procurement_operation_date","boq_vp_procurement_date","boq_manager_project_planning_date",
    "pr_number","pr_item","pr_value","pr_create_date","po_number","po_item","po_create_date","po_value",
    "row_version"
]

def _get_mark(context):
    return context.get("run_id") or "manual__" + datetime.now().strftime("%Y%m%d%H%M%S")

def log_status(process_name, mark, status, error_message=None):
    pg_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dag_name = f"{PG_SCHEMA}.{PG_TABLE}"
    if status == "pending":
        cur.execute(
            f"""
            INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
        )
    else:
        cur.execute(
            f"""
            UPDATE {LOG_TABLE}
               SET status = %s, end_time = %s, error_message = %s
             WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
            """,
            (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
        )
    conn.commit()
    cur.close()
    conn.close()

# --------------------
# Postgres DDL (schema/table)
# --------------------
PG_SQL_CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};"

PG_SQL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{PG_TABLE} (
    project_id text NOT NULL,
    spmk_number text NOT NULL,
    spmk_cancel text NULL,
    spmk_value numeric(18,2) NULL,
    regional text NULL,
    spmk_status text NULL,
    spmk_sow text NULL,
    spmk_approved_date timestamp NULL,
    spmk_create_date timestamp NULL,
    boq_number text NULL,
    boq_status text NULL,
    boq_sow text NULL,
    boq_standard_value numeric(18,2) NULL,
    boq_addwork_value numeric(18,2) NULL,
    boq_value numeric(18,2) NULL,
    boq_create_date timestamp NULL,
    boq_approved_date timestamp NULL,
    boq_draft_date timestamp NULL,
    boq_rejected_date timestamp NULL,
    boq_pm_ro_date timestamp NULL,
    boq_manager_codm_ro_date timestamp NULL,
    boq_gm_area_date timestamp NULL,
    boq_manager_construction_project_deployment_date timestamp NULL,
    boq_gm_construction_project_management_date timestamp NULL,
    boq_manager_procurement_operation_date timestamp NULL,
    boq_vp_procurement_date timestamp NULL,
    boq_manager_project_planning_date timestamp NULL,
    pr_number text NULL,
    pr_item text NULL,
    pr_value numeric(18,2) NULL,
    pr_create_date timestamp NULL,
    po_number text NULL,
    po_item text NULL,
    po_create_date timestamp NULL,
    po_value numeric(18,2) NULL,
    row_version timestamp NOT NULL,
    CONSTRAINT {PG_TABLE}_pk PRIMARY KEY (project_id, spmk_number)
);
"""

PG_SQL_INDEX_ROWVER = f"""
CREATE INDEX IF NOT EXISTS {PG_TABLE}_rowver_idx
ON {PG_SCHEMA}.{PG_TABLE} (row_version);
"""

def create_pg_schema_table(**context):
    mark = _get_mark(context)
    process = "pg_create_schema_table"
    log_status(process, mark, "pending")
    try:
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute(PG_SQL_CREATE_SCHEMA)
        cur.execute(PG_SQL_CREATE_TABLE)
        cur.execute(PG_SQL_INDEX_ROWVER)
        conn.commit()
        cur.close()
        conn.close()
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

def get_pg_last_version(**context):
    """
    Read watermark from Postgres (max(row_version)) or '1970-01-01 00:00:00' if none.
    Honor DM_SPMK_FORCE_FULL for full reload.
    """
    mark = _get_mark(context)
    process = "pg_get_last_version"
    log_status(process, mark, "pending")
    try:
        force_full = (Variable.get(FORCE_FULL_VAR, default_var="false").lower() == "true")
        if force_full:
            context['ti'].xcom_push(key="last_version", value="1970-01-01 00:00:00")
            log_status(process, mark, "success")
            return

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT max(row_version) FROM {PG_SCHEMA}.{PG_TABLE}")
        row = cur.fetchone()
        last_version = row[0].strftime("%Y-%m-%d %H:%M:%S") if row and row[0] else "1970-01-01 00:00:00"
        cur.close()
        conn.close()

        context['ti'].xcom_push(key="last_version", value=last_version)
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

def upsert_from_clickhouse(**context):
    """
    Select from SECOND ClickHouse (dwh_spmk.fact_const2) using PG watermark,
    keep only the latest row per (project_id, spmk_number), then UPSERT into Postgres.
    """
    mark = _get_mark(context)
    process = "pg_upsert_from_clickhouse"
    log_status(process, mark, "pending")
    try:
        last_version = context['ti'].xcom_pull(key="last_version")

        # 1) Build SELECT that returns only the newest row per PK
        ch = ClickHouseHook(clickhouse_conn_id=CH_CONN_ID).get_conn()
        collist = ", ".join(COLUMNS)

        select_sql = f"""
        WITH src AS (
            SELECT
                {collist},
                row_number() OVER (
                    PARTITION BY project_id, spmk_number
                    ORDER BY row_version DESC
                ) AS rn
            FROM {CH_DB}.{CH_TABLE}
            WHERE row_version > toDateTime('{last_version}')
        )
        SELECT {collist}
        FROM src
        WHERE rn = 1
        ORDER BY row_version
        """

        # 2) Prepare Postgres UPSERT
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        cols_pg = ", ".join([f'"{c}"' for c in COLUMNS])
        update_sets = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in COLUMNS if c not in ("project_id", "spmk_number")])

        upsert_sql = f"""
            INSERT INTO {PG_SCHEMA}.{PG_TABLE} ({cols_pg})
            VALUES %s
            ON CONFLICT (project_id, spmk_number)
            DO UPDATE SET {update_sets}
        """

        total = 0
        buffer = []

        # 3) Stream rows from ClickHouse in chunks and upsert
        for row in ch.execute_iter(select_sql, settings={"max_block_size": FETCH_CHUNK}):
            buffer.append(row)
            if len(buffer) >= UPSERT_CHUNK:
                execute_values(cur, upsert_sql, buffer, template=None, page_size=UPSERT_CHUNK)
                conn.commit()
                total += len(buffer)
                buffer.clear()
                logging.info(f"[upsert] committed total={total}")

        if buffer:
            execute_values(cur, upsert_sql, buffer, template=None, page_size=UPSERT_CHUNK)
            conn.commit()
            total += len(buffer)

        cur.close()
        conn.close()
        logging.info(f"[done] Upsert to Postgres complete. Total rows: {total}")
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=CATCHUP,
    dagrun_timeout=timedelta(hours=6),
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=TAGS,
) as dag:

    t_create_pg = PythonOperator(
        task_id="create_pg_schema_table",
        python_callable=create_pg_schema_table,
        provide_context=True,
    )

    t_get_last = PythonOperator(
        task_id="get_pg_last_version",
        python_callable=get_pg_last_version,
        provide_context=True,
    )

    t_upsert = PythonOperator(
        task_id="upsert_from_clickhouse",
        python_callable=upsert_from_clickhouse,
        provide_context=True,
    )

    t_create_pg >> t_get_last >> t_upsert
