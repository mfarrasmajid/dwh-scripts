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
TARGET_DATABASE = "dwh_tms"
TARGET_TABLE = "fact_sitevisitrequest"
FILE_PATH = "/tmp/debug_DWH_tms_factvisitrequest.json"
DAG_ID = "DWH_tms_factvisitrequest"
DAG_INTERVAL = "2-59/5 * * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data WareHouse"

EXTRACT_QUERY = """
        SELECT 
            a.name AS VisitRequest, 
            a.creation, 
            a.person_in_charge, 
            a.pic_contact_mail, 
            a.mobile_tms,
            b.role_unit, 
            c.department_name,
            now() as updated_at
        FROM tms.tabsitevisitrequest a FINAL
        LEFT JOIN tms.tabuser b ON a.pic_contact_mail = b.name
        LEFT JOIN tms.tabdepartment c ON b.role_unit = c.name
        WHERE a.mobile_tms = '1'
        """

SOURCE_COLUMNS=['VisitRequest', 'creation', 'person_in_charge',
                'pic_contact_mail', 'mobile_tms', 'role_unit', 
                'department_name', 'updated_at']

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
            row[0], clean_submit_date(row[1]), row[2].replace("'", "") if row[2] else None, row[3], row[4], row[5], row[6], now
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
