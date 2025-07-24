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
SOURCE_DATABASE = "om"
SOURCE_TABLE = "tabrequestcorrmo"
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_om"
TARGET_TABLE = "fact_requestcorrmo"
FILE_PATH = "/tmp/debug_DWH_om_reqcorrmo.json"
DAG_ID = "DWH_om_requestcorrmo"
DAG_INTERVAL = "32 17 * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"

EXTRACT_QUERY = """
SELECT DISTINCT 
    name, tower_id, site_id, tenant_id, pid,
    tower_name, request_type, detail_request_type, total_amount, currency,
    status, no_spk, mitra_om, regional, area,
    province, district_or_city, address, maintenance_zone, kecamatan,
    tower_owner, customer, date, 
    tt_reference, mo_reference, cf_reference, tt_from, severity,
    case_type, case_category, expenditure_type, portofolio,
    pr_reference, po_reference, po_item, bapp_number, bast_number, gr_status,
    timestamp_spk, ts_pr, ts_submit_report, ts_ready_bast, ts_po,
    ts_submit_boq, ts_approve_boq, return_notes, reason, claim_insurance_created, spk_created,
    creation, modified, ts_submit_vendor, ts_boq_input, ts_approve_ksm,
    ts_approve_sm, ts_approve_mgr_sm, ts_approve_gm_area, ts_approve_mgr_om_hq, ts_approve_sgm_om,
    ts_create_pr, ts_negosiasi, ts_create_po, ts_ready_to_bast, ts_rejected,
    ts_completed, ts_revisi, ts_return, modified_by, owner
    FROM om.tabrequestcorrmo   
"""

SOURCE_COLUMNS=['name','tower_id','site_id','tenant_id','pid',
                'tower_name','request_type','detail_request_type','total_amount','currency',
                'status','no_spk','mitra_om','regional','area',
                'province','district_or_city','address','maintenance_zone','kecamatan',
                'tower_owner','customer','date',
                'tt_reference','mo_reference','cf_reference','tt_from','severity',
                'case_type','case_category','expenditure_type','portofolio',
                'pr_reference','po_reference','po_item','bapp_number','bast_number','gr_status',
                'timestamp_spk','ts_pr','ts_submit_report','ts_ready_bast','ts_po',
                'ts_submit_boq','ts_approve_boq','return_notes','reason','claim_insurance_created', 'spk_created',
                'creation','modified','ts_submit_vendor','ts_boq_input','ts_approve_ksm',
                'ts_approve_sm','ts_approve_mgr_sm','ts_approve_gm_area','ts_approve_mgr_om_hq','ts_approve_sgm_om',
                'ts_create_pr','ts_negosiasi','ts_create_po','ts_ready_to_bast','ts_rejected',
                'ts_completed','ts_revisi','ts_return','modified_by','owner']

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

def clean_date(date_str):
    if not date_str:
        return None
    try:
        # Convert to datetime and remove milliseconds
        return date_str.split('.')[0]
    except ValueError as e:
        logging.error(f"Error parsing date: {date_str} - {e}")
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
            row[21], row[22], row[23], row[24], row[25], row[26], row[27],
            row[28], row[29], row[30], row[31], row[32], row[33], row[34],
            row[35], row[36], clean_date(row[37]), clean_date(row[38]), clean_date(row[39]), clean_date(row[40]), clean_date(row[41]),
            clean_date(row[42]), clean_date(row[43]), row[44], row[45], row[46], row[47], row[48],
            row[49], clean_date(row[50]), clean_date(row[51]), clean_date(row[52]), clean_date(row[53]), clean_date(row[54]), clean_date(row[55]),
            clean_date(row[56]), clean_date(row[57]), clean_date(row[58]), clean_date(row[59]), clean_date(row[60]), clean_date(row[61]), clean_date(row[62]),
            clean_date(row[63]), clean_date(row[64]), clean_date(row[65]), row[66], row[67], row[68]
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

