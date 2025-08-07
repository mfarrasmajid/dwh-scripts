import json
import pandas as pd
import random
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

SOURCE_CONN_ID = "clickhouse_mitratel"
SOURCE_DATABASE = "tms"
SOURCE_TABLE = ""
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_spmk"
TARGET_TABLE = "fact_project"
FILE_PATH = "/tmp/debug_DWH_spmk_factproject.json"
DAG_ID = "DWH_spmk_factproject"
DAG_INTERVAL = "40 17 * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"

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

SOURCE_COLUMNS=[
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
        'status_ebast',
        'create_date_ebast',
        'approved_date_ebast',
        'nomor_gr',
        'status_gr',
        'updated_at']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1, 0, 0),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval= DAG_INTERVAL,
    default_args=default_args,
    catchup=False
)

def log_status(process_name, mark, status, error_message=None):
    """Insert or update the log table."""
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

def clean_submit_date(submit_date_str):
    if not submit_date_str:
        return None
    try:
        # Convert to datetime and remove milliseconds
        return submit_date_str.split('.')[0]
    except ValueError as e:
        logging.error(f"Error parsing submit_date: {submit_date_str} - {e}")
        return None
    
def extract_from_clickhouse(**kwargs):
    """Extract data from ClickHouse and store it as a DataFrame."""
    random_value = random.randint(1000, 9999)  # Angka acak 4 digit
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format YYYYMMDDHHMMSS
    RANDOM_VALUE = f"{random_value}_{timestamp}"
    process_name = "extract_clickhouse"
    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
        conn = clickhouse_hook.get_conn()
        data = conn.execute(EXTRACT_QUERY)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_new = [(
            row[0], row[1], row[2], row[3], row[4], row[5],  row[6], 
            row[7], row[8], row[9], row[10], row[11], row[12],  row[13], 
            row[14], row[15], row[16], row[17], row[18], row[19],  row[20], 
            row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29],now
        ) for row in data]
        df = pd.DataFrame(data_new, columns=SOURCE_COLUMNS)

        df.to_json(FILE_PATH, orient="records", default_handler=str)

        kwargs['ti'].xcom_push(key="file_path", value=FILE_PATH)
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def load_to_clickhouse(**kwargs):
    """Load extracted data into ClickHouse."""
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_clickhouse", key="random_value")
    process_name = "load_clickhouse"
    log_status(process_name, random_value, "pending")

    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids="extract_clickhouse", key="file_path")
    
        if not file_path:
            raise ValueError("No file path received from ClickHouse extraction.")
    
        with open(file_path, "r") as f:
            data_dict = json.load(f)

        df = pd.DataFrame(data_dict, dtype=str)  
        df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
        df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)

        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        hook.execute(f"TRUNCATE TABLE `{TARGET_DATABASE}`.`{TARGET_TABLE}`")

        insert_query = f"INSERT INTO `{TARGET_DATABASE}`.`{TARGET_TABLE}` VALUES"
        values = ', '.join(
            f"""({', '.join(f"'{x}'" for x in row)})""" 
            for row in df.itertuples(index=False, name=None)
        )
        
        hook.execute(insert_query + values)
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

extract_clickhouse = PythonOperator(
    task_id="extract_clickhouse",
    python_callable=extract_from_clickhouse,
    do_xcom_push=False,
    dag=dag,
)

load_clickhouse = PythonOperator(
    task_id="load_clickhouse",
    python_callable=load_to_clickhouse,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

extract_clickhouse >> load_clickhouse
