import json
import pandas as pd
import random
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime

SOURCE_CONN_ID = "clickhouse_mitratel"
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_spmk"
TARGET_TABLE = "fact_const_2"
FILE_PATH = "/tmp/debug_DWH_spmk_factconst2"
DAG_ID = "DWH_spmk_factconst2"
DAG_INTERVAL = "*/5 * * * *"
BATCH_SIZE = 500000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"
TAGS = ["dwh", "spmk", "factconst2"]

EXTRACT_QUERY = """
    SELECT DISTINCT
        a.project_id AS project_id, 
        a.parent AS spmk_number, 
        a.is_cancel AS spmk_cancel, 
        a.pid_total_amount AS spmk_value,
        b.workflow_state AS spmk_status, 
        b.scope_of_work AS spmk_sow, 
        b.approved_date AS spmk_approved_date,
        b.creation AS spmk_create_date,
        c.name AS boq_number, 
        c.workflow_state AS boq_status, 
        c.region AS regional,
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
        FROM spmk.tabboqactual
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
            creation
        FROM spmk.tabmaterialrequestitem
        ) d
        ON d.spmk_number = c.spmk_number
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
"""

SOURCE_COLUMNS = [
  'project_id',
  'spmk_number',
  'spmk_cancel',
  'spmk_value',
  'spmk_status',
  'spmk_sow',
  'spmk_approved_date',
  'spmk_create_date',
  'boq_number',
  'boq_status',
  'regional',
  'boq_sow',
  'boq_standard_value',
  'boq_addwork_value',
  'boq_value',
  'boq_create_date',
  'boq_approved_date',
  'boq_draft_date',
  'boq_rejected_date',
  'boq_pm_ro_date',
  'boq_manager_codm_ro_date',
  'boq_gm_area_date',
  'boq_manager_construction_project_deployment_date',
  'boq_gm_construction_project_management_date',
  'boq_manager_procurement_operation_date',
  'boq_vp_procurement_date',
  'boq_manager_project_planning_date',
  'pr_number',
  'pr_item',
  'pr_value',
  'pr_create_date',
  'po_number',
  'po_item',
  'po_create_date',
  'po_value' ]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1, 0, 0),
}

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
            """, (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
        )
    else:
        cursor.execute(
            f"""
            UPDATE {LOG_TABLE} SET status = %s, end_time = %s, error_message = %s
            WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
            """, (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
        )
    conn.commit()
    cursor.close()
    conn.close()

def get_total_rows():
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
    conn = clickhouse_hook.get_conn()
    count_query = f"SELECT count() FROM ({EXTRACT_QUERY})"
    total_rows = conn.execute(count_query)[0][0]
    return total_rows

@dag(
    dag_id=DAG_ID,
    schedule_interval=DAG_INTERVAL,
    default_args=default_args,
    catchup=False, tags=TAGS
)
def factconst_parallel_dag():
    @task
    def truncate_table():
        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        truncate_query = f"TRUNCATE TABLE `{TARGET_DATABASE}`.`{TARGET_TABLE}`"
        logging.warning(f"Truncating table {TARGET_DATABASE}.{TARGET_TABLE}")
        hook.execute(truncate_query)

    @task
    def get_total_rows_task():
        process_name = "get_total_rows"
        random_value = random.randint(1000, 9999)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        RANDOM_VALUE = f"{random_value}_{timestamp}"
        log_status(process_name, RANDOM_VALUE, "pending")
        try:
            total_rows = get_total_rows()
            log_status(process_name, RANDOM_VALUE, "success")
            return {"total_rows": total_rows, "random_value": RANDOM_VALUE}
        except Exception as e:
            log_status(process_name, RANDOM_VALUE, "failed", str(e))
            raise

    @task
    def get_batch_numbers(meta: dict):
        total_rows = meta["total_rows"]
        batch_count = (total_rows // BATCH_SIZE) + (1 if total_rows % BATCH_SIZE else 0)
        logging.warning(f"Calculated batch_count={batch_count}")
        return list(range(batch_count))

    @task
    def extract_batch(batch_number: int, meta: dict):
        random_value = meta["random_value"]
        process_name = f"extract_batch_{batch_number}"
        log_status(process_name, random_value, "pending")
        try:
            logging.warning(f"Connecting to ClickHouse for extraction, batch_number={batch_number}")
            clickhouse_hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
            conn = clickhouse_hook.get_conn()
            offset = batch_number * BATCH_SIZE
            batch_query = EXTRACT_QUERY + f" LIMIT {BATCH_SIZE} OFFSET {offset}"
            logging.warning(f"Running query: {batch_query}")
            data = conn.execute(batch_query)
            logging.warning(f"Fetched {len(data)} rows for batch_number={batch_number}")
            df = pd.DataFrame(data, columns=SOURCE_COLUMNS)
            file_path = f"{FILE_PATH}_{batch_number}.json"
            df.to_json(file_path, orient="records", default_handler=str)
            logging.warning(f"Saved batch {batch_number} to {file_path}")
            log_status(process_name, random_value, "success")
            return file_path
        except Exception as e:
            logging.error(f"Error in extract_batch {batch_number}: {e}")
            log_status(process_name, random_value, "failed", str(e))
            raise

    @task
    def load_batch(batch_number: int, meta: dict):
        random_value = meta["random_value"]
        process_name = f"load_batch_{batch_number}"
        file_path = f"{FILE_PATH}_{batch_number}.json"
        log_status(process_name, random_value, "pending")
        try:
            logging.warning(f"Loading data from {file_path} for batch_number={batch_number}")
            with open(file_path, "r") as f:
                data_dict = json.load(f)
            if not data_dict:
                logging.warning(f"No data to load for batch_number={batch_number}")
                log_status(process_name, random_value, "success")
                return
            df = pd.DataFrame(data_dict, dtype=str)
            df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
            df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)
            hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
            insert_query = f"INSERT INTO `{TARGET_DATABASE}`.`{TARGET_TABLE}` VALUES"
            values = ', '.join(
                f"""({', '.join(f"'{x}'" for x in row)})""" 
                for row in df.itertuples(index=False, name=None)
            )
            logging.warning(f"Inserting {len(df)} rows into ClickHouse for batch_number={batch_number}")
            hook.execute(insert_query + values)
            log_status(process_name, random_value, "success")
        except Exception as e:
            logging.error(f"Error in load_batch {batch_number}: {e}")
            log_status(process_name, random_value, "failed", str(e))
            raise

    meta = get_total_rows_task()
    batch_numbers = get_batch_numbers(meta)
    extract = extract_batch.partial(meta=meta).expand(batch_number=batch_numbers)
    truncate = truncate_table()
    load_tasks = load_batch.partial(meta=meta).expand(batch_number=batch_numbers)
    meta >> batch_numbers >> extract >> truncate >> load_tasks

factconst_parallel_dag()