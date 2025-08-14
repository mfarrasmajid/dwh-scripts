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
    SELECT
        a.name AS project_id,
        a.site_id_tenant AS site_id_tenant, 
        a.site_name_tenant AS site_name, 
        a.area AS area, 
        a.region AS regional,
        a.status AS status_progress,
        b.parent AS no_spmk,
        bc.scope_of_work AS sow_spmk,
        b.creation AS create_date_spmk,
        bc.approved_date AS approved_date_spmk,
        c.name AS nomor_boq,
        c.workflow_state AS status_boq,
        c.creation AS create_date_boq,
        c.approved_date AS approved_date_boq,
        c.total_spmk AS nominal_spmk,
        c.total_standard_items AS nominal_standar,
        c.total_corrective_items AS nominal_addwork,
        c.total_standard_and_corrective_items AS nominal_total,
        d.parent AS nomor_pr,
        d.creation AS create_date_pr,
        e.parent AS nomor_po,
        e.creation AS create_date_po,
        ef.aprroved_date AS approved_date_po,
        ef.workflow_state AS status_po,
        ef.grand_total AS nominal_po,
        f.name as nomor_bast,
        f.workflow_state AS status_ebast,
        f.creation AS create_date_ebast,
        f.gm_approve_date AS approved_date_ebast,
        g.gr_sap_number AS nomor_gr,
        g.gr_sap_status AS status_gr
    FROM spmk.tabproject a
    LEFT JOIN spmk.tabspmkchild b ON
        b.project_id = a.name 
    LEFT JOIN spmk.tabspmk bc ON
        bc.name = b.parent
    LEFT JOIN spmk.tabboqactual c ON
        c.project_id = a.name
    LEFT JOIN spmk.tabmaterialrequestitem d ON
        d.project = a.name
    LEFT JOIN spmk.tabpurchaseorderitem e ON
        e.pr_number = d.parent
    LEFT JOIN spmk.tabpurchaseorder ef ON
        ef.name = e.parent
    LEFT JOIN spmk.tabbast1 f ON
        f.purchase_orderpo_no = e.parent
    LEFT JOIN spmk.tabgoodreceipt g ON
        g.po_number = e.parent
    WHERE c.is_spmk = '1' 
"""

SOURCE_COLUMNS = [
    'project_id',
    'site_id_tenant', 
    'site_name', 
    'area', 
    'regional',
    'status_progress',
    'no_spmk',
    'sow_spmk',
    'create_date_spmk',
    'approved_date_spmk',
    'nomor_boq',
    'status_boq',
    'create_date_boq',
    'approved_date_boq',
    'nominal_spmk',
    'nominal_standar',
    'nominal_addwork',
    'nominal_total',
    'nomor_pr',
    'create_date_pr',
    'nomor_po',
    'create_date_po',
    'approved_date_po',
    'status_po',
    'nominal_po',
    'nomor_bast',
    'status_ebast',
    'create_date_ebast',
    'approved_date_ebast',
    'nomor_gr',
    'status_gr'
]

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
            df = df.where(pd.notnull(df), '')
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
            df = df.where(pd.notnull(df), '')
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
    extract_batch.partial(meta=meta).expand(batch_number=batch_numbers)
    load_batch.partial(meta=meta).expand(batch_number=batch_numbers)

factconst_parallel_dag()