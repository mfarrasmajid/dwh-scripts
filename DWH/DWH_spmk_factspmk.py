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
TARGET_TABLE = "fact_spmk"
FILE_PATH = "/tmp/debug_DWH_spmk_factspmk.json"
DAG_ID = "DWH_spmk_factspmk"
DAG_INTERVAL = "32 17 * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"
TAGS = ["dwh", "spmk", "factspmk"]

EXTRACT_QUERY = """
SELECT DISTINCT 
    a.parent AS wo_number,
    a.project_id,
    a.area,
    a.region,
    a.province,
    b.parent AS spmk_number,
    b.is_cancel AS is_spmk_cancel,
    bb.workflow_state AS spmk_status,
    c.name AS task_number,
    c.subject AS task_desc,
    c.type AS task_type,
    c.status AS task_status,
    c.bast_document,
    d.name AS boq,
    d.workflow_state AS boq_status,
    d.mitra,
    d.scope_of_work,
    e.name AS bast_name,
    e.nomor_bast,
    e.workflow_state AS bast_status,
    e.pekerjaan,
    f.parent AS po_number,
    ff.workflow_state AS po_status,
    g.gr_sap_status
    FROM (
        SELECT parent, project_id, area, region, province FROM spmk.tabwoandpid
        UNION ALL
        SELECT parent, project_id, area, region, province FROM spmk.tabnewwo
        UNION ALL
        SELECT parent, project_id, area, region, province FROM spmk.tabwositelistcolo
    ) a
    LEFT JOIN spmk.tabspmkchild b 
        ON a.project_id = b.project_id
    LEFT JOIN spmk.tabspmk bb 
        ON b.parent = bb.spmk_number
    LEFT JOIN spmk.tabtask c
        ON a.project_id = c.project
    LEFT JOIN spmk.tabboqactual d 
        ON a.project_id = d.project_id
        AND (
            (d.scope_of_work LIKE '%CME%' AND c.subject LIKE '%CME%')
            OR 
            (d.scope_of_work LIKE '%Perkuatan%' AND c.subject LIKE '%Perkuatan%')
        )
    LEFT JOIN spmk.tabbast1 e 
        ON a.project_id = e.project_id
        AND d.scope_of_work = e.pekerjaan
    LEFT JOIN spmk.tabpurchaseorderitem f 
        ON a.project_id = f.project
        AND e.purchase_orderpo_no = f.parent
    LEFT JOIN spmk.tabpurchaseorder ff 
        ON ff.name = e.purchase_orderpo_no
    LEFT JOIN spmk.tabgoodreceipt g 
        ON a.project_id = g.project_id
        AND e.purchase_orderpo_no = g.po_number
        AND g.gr_sap_status = 'Success' AND g.gr_sap_number IS NOT NULL
    WHERE 
        (g.gr_sap_status IS NULL OR TRIM(g.gr_sap_status) <> '')
        AND a.project_id LIKE '24%'
        AND c.subject LIKE 'BAST%'
        AND (c.subject LIKE '%CME%' OR c.subject LIKE '%Perkuatan%')
"""

SOURCE_COLUMNS=['wo_number', 'project_id', 'area', 'region', 
                'province', 'spmk_number', 'is_spmk_cancel', 
                'spmk_status', 'task_number', 'task_desc', 
                'task_type', 'task_status', 'bast_document', 
                'boq', 'boq_status', 'mitra', 'scope_of_work', 
                'bast_name', 'nomor_bast', 'bast_status', 'pekerjaan', 
                'po_number', 'po_status', 'gr_sap_status', 'updated_at']

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
    catchup=False, tags=TAGS
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
            row[21], row[22], row[23], now
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
