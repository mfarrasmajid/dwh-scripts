import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

SOURCE_CONN_ID = "clickhouse_dwh_mitratel"
SOURCE_DATABASE = "dwh_om"
SOURCE_TABLE = "fact_requestcorrmo"
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "datamart"
TARGET_TABLE = "mart_om_requestcorrmo"
FILE_PATH = "/tmp/debug_DM_om_requestcorrmo.json"
DAG_ID = "DM_om_requestcorrmo"
DAG_INTERVAL = "5-59/15 * * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"

EXTRACT_QUERY = """
SELECT
    id, tower_id, site_id, tenant_id, pid, 
    request_type, detail_request_type, total_amount, status, mitra_om,
    regional, area, expenditure_type, portofolio, spk_created, ts_submit_vendor
FROM dwh_om.fact_requestcorrmo FINAL
WHERE toYear(parseDateTimeBestEffort(ts_submit_vendor)) IN ('2024','2025') 
"""

SOURCE_COLUMNS=['id','tower_id','site_id','tenant_id','pid',
                'request_type','detail_request_type','total_amount','status','mitra_om',
                'regional','area','expenditure_type','portofolio','spk_created','ts_submit_vendor']

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

    
def update_expenditure_type(request_type):
    if request_type == "Corrective":
        return "CAPEX"
    elif request_type in ["Jasa", "MBP Over Usage", "BBM PAK & IPLH", "CSR", "Penjaga Site"]:
        return "BODP"
    elif request_type in ["Imbas Petir", "Insurance Claim"]:
        return "BDD"
    else:
        print(f"[WARNING] Unmapped request_type: {request_type}")
        return "UNKNOWN" 

  
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
        
        processed_data = []
        for row in data:
            row = list(row)

            row[12] = update_expenditure_type(row[5]); #Mapping expenditure type based on Function update_expenditure_type
            #print(f"Mapping request_type='{row[5]}' => expenditure_type='{row[12]}'")
            processed_data.append(tuple(row))

        df = pd.DataFrame(processed_data, columns=SOURCE_COLUMNS).astype(str)

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
        df = df.replace({'': None, 'None': None})

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO public.mart_om_requestcorrmo (
            id,tower_id,site_id,tenant_id,pid,
            request_type,detail_request_type,total_amount,status,mitra_om,
            regional,area,expenditure_type,portofolio,spk_created,ts_submit_vendor
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET 
            tower_id = EXCLUDED.tower_id,
            site_id = EXCLUDED.site_id,
            tenant_id = EXCLUDED.tenant_id,
            pid = EXCLUDED.pid,
            request_type = EXCLUDED.request_type,
            detail_request_type = EXCLUDED.detail_request_type,
            total_amount = EXCLUDED.total_amount,
            status = EXCLUDED.status,
            mitra_om = EXCLUDED.mitra_om,
            regional = EXCLUDED.regional,
            area = EXCLUDED.area,
            expenditure_type = EXCLUDED.expenditure_type,
            portofolio = EXCLUDED.portofolio,
            spk_created = EXCLUDED.spk_created,
            ts_submit_vendor = EXCLUDED.ts_submit_vendor,
            updated_at = NOW() + INTERVAL '7 hours'
        """

        for __, row in df.iterrows():
            cursor.execute(insert_query, (row['id'], row['tower_id'], row['site_id'], 
                                               row['tenant_id'], row['pid'], 
                                               row['request_type'], row['detail_request_type'],
                                               row['total_amount'], row['status'], row['mitra_om'], 
                                               row['regional'],row['area'],row['expenditure_type'], 
                                               row['portofolio'],row['spk_created'],row['ts_submit_vendor']
                                               ))
        conn.commit()
        
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
