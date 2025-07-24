import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

SOURCE_CONN_ID = "clickhouse_mitratel"
SOURCE_DATABASE = "tms"
SOURCE_TABLE = ""
TARGET_CONN_ID = "db_dwh_mitratel"
TARGET_DATABASE = "dwh_mitratel"
TARGET_TABLE = "fact_sitevisitrequest"
FILE_PATH = "/tmp/debug_DWH_tms_factvisitrequest.json"
DAG_ID = "factvisitrequest"
DAG_INTERVAL = "@daily"
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
            c.department_name
        FROM tms.tabsitevisitrequest a
        LEFT JOIN tms.tabuser b ON a.pic_contact_mail = b.name
        LEFT JOIN tms.tabdepartment c ON b.role_unit = c.name
        WHERE a.mobile_tms = '1'
        """

SOURCE_COLUMNS=['VisitRequest', 'creation', 'person_in_charge',
                'pic_contact_mail', 'mobile_tms', 'role_unit', 
                'department_name']

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
        df = pd.DataFrame(data, columns=SOURCE_COLUMNS)

        df.to_json(FILE_PATH, orient="records", default_handler=str)

        kwargs['ti'].xcom_push(key="file_path", value=FILE_PATH)
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def load_to_postgres(**kwargs):
    """Load extracted data into Postgres."""
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_clickhouse", key="random_value")
    process_name = "load_postgres"
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

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        insert_query = """
        INSERT INTO public.fact_sitevisitrequest (
        visitrequest, creation, person_in_charge, 
        pic_contact_mail, mobile_tms, role_unit, 
        department_name, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now() + INTERVAL '7 hours')
        ON CONFLICT (visitrequest) 
        DO UPDATE SET 
            creation = EXCLUDED.creation,
            person_in_charge = EXCLUDED.person_in_charge,
            pic_contact_mail = EXCLUDED.pic_contact_mail,
            mobile_tms = EXCLUDED.mobile_tms,
            role_unit = EXCLUDED.role_unit,
            department_name = EXCLUDED.department_name,
            updated_at = now() + INTERVAL '7 hours';
        """
        
        for __, row in df.iterrows():
            hook.run(insert_query, parameters=(row['VisitRequest'], row['creation'], row['person_in_charge'], 
                                               row['pic_contact_mail'], row['mobile_tms'], 
                                               row['role_unit'], row['department_name']))
        
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

load_postgres = PythonOperator(
    task_id="load_postgres",
    python_callable=load_to_postgres,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

extract_clickhouse >> load_postgres
