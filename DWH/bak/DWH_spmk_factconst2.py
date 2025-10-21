# dags/dwh_spmk_factconst2.py
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# --------------------
# CONFIG
# --------------------
DAG_ID = "DWH_spmk_factconst2"
SCHEDULE = "7-59/15 * * * *"   # every 15 minutes starting at minute 7
CATCHUP = False
START_DATE = datetime(2025, 1, 1)

SOURCE_CONN_ID = "clickhouse_mitratel"
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_spmk"
TARGET_TABLE = "fact_const2"

FORCE_FULL_VAR = "DM_SPMK_FORCE_FULL"

# Logging (same shape as your previous DAG)
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"

TAGS = ["dm", "spmk", "clickhouse", "c2c"]

# Streaming batch size (rows)
FETCH_CHUNK = 100_000

def _get_mark(context):
    run_id = context.get("run_id") or "manual__" + datetime.now().strftime("%Y%m%d%H%M%S")
    return run_id

def log_status(process_name, mark, status, error_message=None):
    pg_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dag_name = f"{TARGET_DATABASE}.{TARGET_TABLE}"
    if status == "pending":
        cursor.execute(
            f"""
            INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
        )
    else:
        cursor.execute(
            f"""
            UPDATE {LOG_TABLE}
               SET status = %s,
                   end_time = %s,
                   error_message = %s
             WHERE process_name = %s
               AND status = 'pending'
               AND dag_name = %s
               AND mark = %s
               AND kategori = %s
            """,
            (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
        )
    conn.commit()
    cursor.close()
    conn.close()

# --------------------
# SQL: SCHEMA (DB & TABLE) on TARGET
# --------------------
SQL_CREATE_DB = f"""
CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE};
"""

SQL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DATABASE}.{TARGET_TABLE}
(
    project_id String,
    spmk_number String,
    spmk_cancel Nullable(String),
    spmk_value Nullable(Decimal(18,2)),
    regional Nullable(String),
    spmk_status Nullable(String),
    spmk_sow Nullable(String),
    spmk_approved_date Nullable(DateTime),
    spmk_create_date Nullable(DateTime),
    boq_number Nullable(String),
    boq_status Nullable(String),
    boq_sow Nullable(String),
    boq_standard_value Nullable(Decimal(18,2)),
    boq_addwork_value Nullable(Decimal(18,2)),
    boq_value Nullable(Decimal(18,2)),
    boq_create_date Nullable(DateTime),
    boq_approved_date Nullable(DateTime),
    boq_draft_date Nullable(DateTime),
    boq_rejected_date Nullable(DateTime),
    boq_pm_ro_date Nullable(DateTime),
    boq_manager_codm_ro_date Nullable(DateTime),
    boq_gm_area_date Nullable(DateTime),
    boq_manager_construction_project_deployment_date Nullable(DateTime),
    boq_gm_construction_project_management_date Nullable(DateTime),
    boq_manager_procurement_operation_date Nullable(DateTime),
    boq_vp_procurement_date Nullable(DateTime),
    boq_manager_project_planning_date Nullable(DateTime),
    pr_number Nullable(String),
    pr_item Nullable(String),
    pr_value Nullable(Decimal(18,2)),
    pr_create_date Nullable(DateTime),
    po_number Nullable(String),
    po_item Nullable(String),
    po_create_date Nullable(DateTime),
    po_value Nullable(Decimal(18,2)),

    row_version DateTime
)
ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMM(row_version)
ORDER BY (project_id, spmk_number)
SETTINGS index_granularity = 8192;
"""

# --------------------
# BASE QUERY (runs on SOURCE) + ROW_VERSION
# --------------------
BASE_QUERY_WITH_VERSION = """
WITH base AS (
    SELECT DISTINCT
      a.project_id AS project_id, 
      a.parent AS spmk_number, 
      a.is_cancel AS spmk_cancel, 
      a.pid_total_amount AS spmk_value,
      a.region AS regional,
      b.workflow_state AS spmk_status, 
      b.scope_of_work AS spmk_sow, 
      b.approved_date AS spmk_approved_date,
      b.creation AS spmk_create_date,
      c.name AS boq_number, 
      c.workflow_state AS boq_status,
      c.scope_of_work AS boq_sow,
      c.total_standard_items AS boq_standard_value, 
      c.total_corrective_items AS boq_addwork_value, 
      c.total_standard_and_corrective_items AS boq_value, 
      c.creation AS boq_create_date, 
      c.approved_date AS boq_approved_date,
      c.draft_date AS boq_draft_date,
      c.rejected_date AS boq_rejected_date,
      c.pm_regional_review_date AS boq_pm_ro_date,
      c.manager_construction_om_deployment_review_date AS boq_manager_codm_ro_date,
      c.gm_review_date AS boq_gm_area_date,
      c.manager_construction_project_deployment_review_date AS boq_manager_construction_project_deployment_date,
      c.gm_construction_project_manager_review_date AS boq_gm_construction_project_management_date,
      c.manager_procurement_operation_review_date AS boq_manager_procurement_operation_date,
      c.vp_procurement_review_date AS boq_vp_procurement_date,
      c.manager_project_planning_review_date AS boq_manager_project_planning_date,
      d.parent AS pr_number,
      d.item_number_of_purchasing_document_sap AS pr_item,
      d.amount AS pr_value,
      d.creation AS pr_create_date,
      f.name AS po_number,
      e.item_number_of_purchasing_document_sap AS po_item,
      e.creation AS po_create_date,
      e.net_amount AS po_value
    FROM spmk.tabspmkchild a
    INNER JOIN (
      SELECT
        name,
        workflow_state,
        scope_of_work,
        approved_date,
        creation
      FROM spmk.tabspmk	
      WHERE workflow_state = 'Approved'
        AND COALESCE(scope_of_work, '') NOT ILIKE '%Fiber%'
    ) b
      ON b.name = a.parent
    LEFT JOIN (
      SELECT
        project_id,
        spmk_number,
        name,
        workflow_state,
        region,
        scope_of_work,
        total_standard_items,
        total_corrective_items,
        total_standard_and_corrective_items,
        creation,
        approved_date,
        draft_date,
        rejected_date,
        pm_regional_review_date,
        manager_construction_om_deployment_review_date,
        gm_review_date,
        manager_construction_project_deployment_review_date,
        gm_construction_project_manager_review_date,
        manager_procurement_operation_review_date,
        vp_procurement_review_date,
        manager_project_planning_review_date,
        is_spmk
      FROM spmk.tabboqactual final
      WHERE is_spmk = '1'
    ) c
      ON c.project_id = a.project_id
     AND c.spmk_number = a.parent
    LEFT JOIN (
      SELECT
        spmk_number,
        parent,
        item_number_of_purchasing_document_sap,
        amount,
        creation,
        project
      FROM spmk.tabmaterialrequestitem
    ) d
      ON d.spmk_number = c.spmk_number
     AND d.project = a.project_id
    LEFT JOIN (
      SELECT
        pr_number,
        parent,
        item_number_of_purchasing_document_sap,
        creation,
        net_amount,
        item_number_of_purchase_requisition
      FROM spmk.tabpurchaseorderitem
    ) e
      ON e.pr_number = d.parent
      AND e.item_number_of_purchase_requisition = d.item_number_of_purchasing_document_sap
    LEFT JOIN (
      SELECT
        name,
        aprroved_date,
        workflow_state
      FROM spmk.tabpurchaseorder
    ) f
      ON f.name = e.parent
),
norm AS (
    SELECT
        project_id,
        spmk_number,
        spmk_cancel,
        toDecimal64OrNull(spmk_value, 2) AS spmk_value,
        regional,
        spmk_status,
        spmk_sow,

        /* Normalize all date-like columns to Nullable(DateTime) */
        parseDateTimeBestEffortOrNull(toString(spmk_approved_date))                              AS spmk_approved_date_dt,
        parseDateTimeBestEffortOrNull(toString(spmk_create_date))                                 AS spmk_create_date_dt,
        boq_number,
        boq_status,
        boq_sow,
        toDecimal64OrNull(boq_standard_value, 2)                                                  AS boq_standard_value,
        toDecimal64OrNull(boq_addwork_value, 2)                                                   AS boq_addwork_value,
        toDecimal64OrNull(boq_value, 2)                                                           AS boq_value,
        parseDateTimeBestEffortOrNull(toString(boq_create_date))                                  AS boq_create_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_approved_date))                                AS boq_approved_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_draft_date))                                   AS boq_draft_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_rejected_date))                                AS boq_rejected_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_pm_ro_date))                                   AS boq_pm_ro_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_manager_codm_ro_date))                         AS boq_manager_codm_ro_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_gm_area_date))                                 AS boq_gm_area_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_manager_construction_project_deployment_date)) AS boq_manager_construction_project_deployment_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_gm_construction_project_management_date))      AS boq_gm_construction_project_management_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_manager_procurement_operation_date))           AS boq_manager_procurement_operation_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_vp_procurement_date))                          AS boq_vp_procurement_date_dt,
        parseDateTimeBestEffortOrNull(toString(boq_manager_project_planning_date))                AS boq_manager_project_planning_date_dt,
        pr_number,
        pr_item,
        toDecimal64OrNull(pr_value, 2)                                                            AS pr_value,
        parseDateTimeBestEffortOrNull(toString(pr_create_date))                                   AS pr_create_date_dt,
        po_number,
        po_item,
        parseDateTimeBestEffortOrNull(toString(po_create_date))                                   AS po_create_date_dt,
        toDecimal64OrNull(po_value, 2)                                                            AS po_value
    FROM base
)
SELECT
    project_id,
    spmk_number,
    spmk_cancel,
    spmk_value,
    regional,
    spmk_status,
    spmk_sow,

    /* use normalized DateTime columns */
    spmk_approved_date_dt        AS spmk_approved_date,
    spmk_create_date_dt          AS spmk_create_date,
    boq_number,
    boq_status,
    boq_sow,
    boq_standard_value,
    boq_addwork_value,
    boq_value,
    boq_create_date_dt           AS boq_create_date,
    boq_approved_date_dt         AS boq_approved_date,
    boq_draft_date_dt            AS boq_draft_date,
    boq_rejected_date_dt         AS boq_rejected_date,
    boq_pm_ro_date_dt            AS boq_pm_ro_date,
    boq_manager_codm_ro_date_dt  AS boq_manager_codm_ro_date,
    boq_gm_area_date_dt          AS boq_gm_area_date,
    boq_manager_construction_project_deployment_date_dt AS boq_manager_construction_project_deployment_date,
    boq_gm_construction_project_management_date_dt     AS boq_gm_construction_project_management_date,
    boq_manager_procurement_operation_date_dt          AS boq_manager_procurement_operation_date,
    boq_vp_procurement_date_dt   AS boq_vp_procurement_date,
    boq_manager_project_planning_date_dt AS boq_manager_project_planning_date,
    pr_number,
    pr_item,
    pr_value,
    pr_create_date_dt            AS pr_create_date,
    po_number,
    po_item,
    po_create_date_dt            AS po_create_date,
    po_value,

    greatest(
        coalesce(spmk_approved_date_dt, toDateTime(0)),
        coalesce(spmk_create_date_dt, toDateTime(0)),
        coalesce(boq_create_date_dt, toDateTime(0)),
        coalesce(boq_approved_date_dt, toDateTime(0)),
        coalesce(boq_draft_date_dt, toDateTime(0)),
        coalesce(boq_rejected_date_dt, toDateTime(0)),
        coalesce(boq_pm_ro_date_dt, toDateTime(0)),
        coalesce(boq_manager_codm_ro_date_dt, toDateTime(0)),
        coalesce(boq_gm_area_date_dt, toDateTime(0)),
        coalesce(boq_manager_construction_project_deployment_date_dt, toDateTime(0)),
        coalesce(boq_gm_construction_project_management_date_dt, toDateTime(0)),
        coalesce(boq_manager_procurement_operation_date_dt, toDateTime(0)),
        coalesce(boq_vp_procurement_date_dt, toDateTime(0)),
        coalesce(boq_manager_project_planning_date_dt, toDateTime(0)),
        coalesce(pr_create_date_dt, toDateTime(0)),
        coalesce(po_create_date_dt, toDateTime(0))
    ) AS row_version
FROM norm
"""

# Column order for both SELECT and INSERT
COLUMN_LIST = (
    "project_id, spmk_number, spmk_cancel, spmk_value, regional, spmk_status, "
    "spmk_sow, spmk_approved_date, spmk_create_date, boq_number, boq_status, "
    "boq_sow, boq_standard_value, boq_addwork_value, boq_value, boq_create_date, "
    "boq_approved_date, boq_draft_date, boq_rejected_date, boq_pm_ro_date, "
    "boq_manager_codm_ro_date, boq_gm_area_date, "
    "boq_manager_construction_project_deployment_date, "
    "boq_gm_construction_project_management_date, "
    "boq_manager_procurement_operation_date, boq_vp_procurement_date, "
    "boq_manager_project_planning_date, pr_number, pr_item, pr_value, "
    "pr_create_date, po_number, po_item, po_create_date, po_value, row_version"
)

def _run_sql(conn_id: str, sql: str):
    client = ClickHouseHook(clickhouse_conn_id=conn_id).get_conn()
    client.execute(sql)

def create_schema_table(**context):
    mark = _get_mark(context)
    process = "create_schema_table"
    log_status(process, mark, "pending")
    try:
        _run_sql(TARGET_CONN_ID, SQL_CREATE_DB)
        _run_sql(TARGET_CONN_ID, SQL_CREATE_TABLE)
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

def get_last_version(**context):
    mark = _get_mark(context)
    process = "get_last_version"
    log_status(process, mark, "pending")
    try:
        client = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID).get_conn()

        force_full = (Variable.get(FORCE_FULL_VAR, default_var="false").lower() == "true")
        if force_full:
            context['ti'].xcom_push(key="last_version", value="1970-01-01 00:00:00")
            log_status(process, mark, "success")
            return

        rows = client.execute(f"SELECT max(row_version) FROM {TARGET_DATABASE}.{TARGET_TABLE}")
        last_version = rows[0][0] if rows and rows[0] else None
        if last_version is None:
            last_version = "1970-01-01 00:00:00"
        elif not isinstance(last_version, str):
            last_version = last_version.strftime("%Y-%m-%d %H:%M:%S")

        context['ti'].xcom_push(key="last_version", value=last_version)
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

FETCH_CHUNK = 100_000  # rows per batch

def upsert_incremental(**context):
    """
    SELECT from SOURCE (filtered by last_version) -> INSERT into TARGET in chunks,
    using clickhouse_driver.Client without cursors.
    """
    mark = _get_mark(context)
    process = "upsert_incremental"
    log_status(process, mark, "pending")
    try:
        last_version = context['ti'].xcom_pull(key="last_version")
        force_full = (Variable.get(FORCE_FULL_VAR, default_var="false").lower() == "true")
        where_clause = "" if force_full else f"WHERE row_version > toDateTime('{last_version}')"

        source = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID).get_conn()
        target = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID).get_conn()

        select_sql = f"""
        SELECT {COLUMN_LIST}
        FROM ({BASE_QUERY_WITH_VERSION}) q
        {where_clause}
        """
        insert_sql = f"INSERT INTO {TARGET_DATABASE}.{TARGET_TABLE} ({COLUMN_LIST}) VALUES"

        total = 0
        buffer = []
        # stream rows from SOURCE
        for row in source.execute_iter(select_sql, settings={'max_block_size': FETCH_CHUNK}):
            buffer.append(row)
            if len(buffer) >= FETCH_CHUNK:
                target.execute(insert_sql, buffer)
                total += len(buffer)
                buffer.clear()

        # flush remainder
        if buffer:
            target.execute(insert_sql, buffer)
            total += len(buffer)

        logging.info(f"[done] Upsert complete. Total rows inserted: {total}")
        log_status(process, mark, "success")
    except Exception as e:
        log_status(process, mark, "failed", str(e))
        raise

def optimize_table(**context):
    mark = _get_mark(context)
    process = "optimize_table"
    log_status(process, mark, "pending")
    try:
        _run_sql(TARGET_CONN_ID, f"OPTIMIZE TABLE {TARGET_DATABASE}.{TARGET_TABLE} FINAL DEDUPLICATE;")
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
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=TAGS,
) as dag:

    t_create = PythonOperator(
        task_id="create_schema_table",
        python_callable=create_schema_table,
        provide_context=True,
    )

    t_get_last = PythonOperator(
        task_id="get_last_version",
        python_callable=get_last_version,
        provide_context=True,
    )

    t_upsert = PythonOperator(
        task_id="upsert_incremental",
        python_callable=upsert_incremental,
        provide_context=True,
    )

    t_optimize = PythonOperator(
        task_id="optimize_table",
        python_callable=optimize_table,
        provide_context=True,
    )

    t_create >> t_get_last >> t_upsert >> t_optimize
