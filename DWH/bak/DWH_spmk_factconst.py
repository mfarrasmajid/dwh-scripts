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
TARGET_TABLE = "fact_const"
FILE_PATH = "/tmp/debug_DWH_spmk_factconst"
DAG_ID = "DWH_spmk_factconst"
DAG_INTERVAL = "45 17 * * *"
BATCH_SIZE = 500000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"
TAGS = ["dwh", "spmk", "factconst"]

EXTRACT_QUERY = """
    SELECT DISTINCT
    a.name                               AS project_id,
    a.site_id_tenant                     AS site_id,
    a.site_name_tenant                   AS site_name,
    a.area                               AS area,
    a.region                             AS regional,
    a.site_status                        AS status_project,
    a.actual_rfi_date                    AS rfi_date, 
    b.parent                             AS spmk_number,
    b.is_cancel                          AS spmk_cancel,
    c.scope_of_work                      AS spmk_sow,
    c.approved_date                      AS spmk_approved_date,
    c.creation                           AS spmk_create_date,
    d.name                               AS boq_number, 
    d.workflow_state                     AS status_boq,
    d.total_spmk                         AS nilai_spmk,
    d.total_standard_items               AS nilai_standar,
    d.total_corrective_items             AS nilai_addwork,
    d.total_standard_and_corrective_items AS nilai_boq,
    d.creation                           AS create_date_boq,
    d.approved_date                      AS approved_date_boq,
    d.draft_date                         AS boq_draft_date,
    d.pm_regional_review_date            AS boq_pm_regional_review_date,
    d.manager_construction_om_deployment_review_date AS boq_manager_construction_om_deployment_review_date,
    d.gm_review_date                     AS boq_gm_review_date,
    d.manager_construction_project_deployment_review_date AS boq_manager_construction_project_deployment_review_date,
    d.gm_construction_project_manager_review_date AS boq_gm_construction_project_manager_review_date,
    d.vp_procurement_review_date         AS boq_vp_procurement_review_date,
    e.parent                             AS pr_number,
    e.item_number_of_purchasing_document_sap AS pr_item,
    e.amount                             AS pr_value,
    e.creation                           AS create_date_pr,  
    g.name                               AS po_number,
    f.item_number_of_purchasing_document_sap AS po_item,
    f.creation                           AS create_date_po,
    f.net_amount                         AS po_value,
    g.aprroved_date                      AS approved_date_po,
    g.workflow_state                     AS status_po,
    h.name                               AS bast_number,
    h.workflow_state                     AS status_ebast,
    h.creation                           AS create_date_ebast,
    h.gm_approve_date                    AS approved_date_ebast,
    h.pm_review_date                     AS bast_pm_review_date,
    h.mgr_review_date                    AS bast_mgr_review_date,
    h.workflow_state                     AS bast_status,
    i.gr_sap_number                      AS nomor_gr,
    i.gr_sap_status                      AS status_gr,
    i.grand_total                        AS value_gr,
    i.creation                           AS gr_create_date,
    i.posting_date                       AS gr_done_date
    FROM
    (
    SELECT
        name,
        site_id_tenant,
        site_name_tenant,
        area,
        region,
        site_status,
        actual_rfi_date
    FROM spmk.tabproject
    ) AS a
    LEFT JOIN
    (
    SELECT DISTINCT
        project_id,
        parent,
        is_cancel
    FROM spmk.tabspmkchild
    ) AS b
    ON b.project_id = a.name
    LEFT JOIN
    (
    SELECT DISTINCT
        name,
        scope_of_work,
        approved_date,
        creation
    FROM spmk.tabspmk
    ) AS c
    ON c.name = b.parent
    LEFT JOIN
    (
    SELECT DISTINCT
        project_id,
        spmk_number,
        name,
        workflow_state,
        total_spmk,
        total_standard_items,
        total_corrective_items,
        total_standard_and_corrective_items,
        creation,
        approved_date,
        is_spmk,
        draft_date,
        pm_regional_review_date,
        manager_construction_om_deployment_review_date,
        gm_review_date,
        manager_construction_project_deployment_review_date,
        gm_construction_project_manager_review_date,
        vp_procurement_review_date
    FROM spmk.tabboqactual
    ) AS d
    ON d.project_id = a.name
    AND d.spmk_number = c.name
    LEFT JOIN
    (
    SELECT DISTINCT
        spmk_number,
        parent,
        item_number_of_purchasing_document_sap,
        amount,
        creation
    FROM spmk.tabmaterialrequestitem
    ) AS e
    ON e.spmk_number = d.spmk_number
    LEFT JOIN
    (
    SELECT DISTINCT
        pr_number,
        parent,
        item_number_of_purchasing_document_sap,
        creation,
        net_amount
    FROM spmk.tabpurchaseorderitem
    ) AS f
    ON f.pr_number = e.parent
    LEFT JOIN
    (
    SELECT DISTINCT
        name,
        aprroved_date,
        workflow_state
    FROM spmk.tabpurchaseorder
    ) AS g
    ON g.name = f.parent
    LEFT JOIN
    (
    SELECT DISTINCT
        name,
        purchase_orderpo_no,
        project_id,
        workflow_state,
        creation,
        gm_approve_date,
        pm_review_date,
        mgr_review_date
    FROM spmk.tabbast1
    ) AS h
    ON h.purchase_orderpo_no = g.name
    AND h.project_id = a.name
    LEFT JOIN
    (
    SELECT DISTINCT
        po_number,
        project_id,
        item_number_of_purchasing_document_sap,
        gr_sap_number,
        gr_sap_status,
        grand_total,
        creation,
        posting_date
    FROM spmk.tabgoodreceipt
    ) AS i
    ON i.po_number = g.name
    AND i.project_id = a.name
    AND i.item_number_of_purchasing_document_sap = f.item_number_of_purchasing_document_sap
    WHERE d.is_spmk = '1'
    AND d.name NOT LIKE '%SPMK%'
"""

SOURCE_COLUMNS = [
  'project_id',
  'site_id',
  'site_name',
  'area',
  'regional',
  'status_project',
  'rfi_date',
  'spmk_number',
  'spmk_cancel',
  'spmk_sow',
  'spmk_approved_date',
  'spmk_create_date',
  'boq_number',
  'status_boq',
  'nilai_spmk',
  'nilai_standar',
  'nilai_addwork',
  'nilai_boq',
  'create_date_boq',
  'approved_date_boq',
  'boq_draft_date',
  'boq_pm_regional_review_date',
  'boq_manager_construction_om_deployment_review_date',
  'boq_gm_review_date',
  'boq_manager_construction_project_deployment_review_date',
  'boq_gm_construction_project_manager_review_date',
  'boq_vp_procurement_review_date',
  'pr_number',
  'pr_item',
  'pr_value',
  'create_date_pr',
  'po_number',
  'po_item',
  'create_date_po',
  'po_value',
  'approved_date_po',
  'status_po',
  'bast_number',
  'status_ebast',
  'create_date_ebast',
  'approved_date_ebast',
  'bast_pm_review_date',
  'bast_mgr_review_date',
  'bast_status',
  'nomor_gr',
  'status_gr',
  'value_gr',
  'gr_create_date',
  'gr_done_date' ]

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