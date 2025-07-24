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

SOURCE_CONN_ID = "clickhouse_dwh_mitratel"
SOURCE_DATABASE = "dwh_tms"
SOURCE_TABLE = "fact_sitevisitreport"
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "datamart"
TARGET_TABLE = "mart_tms_visitreport_province"
FILE_PATH = "/tmp/debug_DM_tms_visitreport.json"
FILE_PATH_2 = "tmp/debug_2_DM_tms_visitreport.json"
DAG_ID = "DM_tms_visitreport"
DAG_INTERVAL = "4-59/5 * * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"

EXTRACT_QUERY = """
        SELECT 
            visitreport, email, user_name, unit_mtel, direktorat_mtel, site_id, 
            site_name, comment_tower, submit_date, site_visit_requet, region, province
        FROM dwh_tms.fact_sitevisitreport FINAL
        """

SOURCE_COLUMNS=['visitreport', 'email', 'user_name', 'unit_mtel', 'direktorat_mtel', 'site_id', 
            'site_name', 'comment_tower', 'submit_date', 'site_visit_requet', 'region', 'province']

SOURCE_COLUMNS_2 = ['visitreport', 'email', 'user_name', 'unit_mtel', 'direktorat_mtel', 'site_id', 
            'site_name', 'comment_tower', 'submit_date', 'site_visit_requet', 'region', 'province', 'province_id']

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

        df = pd.DataFrame(data, columns=SOURCE_COLUMNS).astype(str)

        df.to_json(FILE_PATH, orient="records", default_handler=str)

        kwargs['ti'].xcom_push(key="file_path", value=FILE_PATH)
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def transform_data(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_clickhouse", key="random_value")
    process_name = "transform_data"
    log_status(process_name, random_value, "pending")

    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids="extract_clickhouse", key="file_path")
    
        if not file_path:
            raise ValueError("No file path received from ClickHouse extraction.")
    
        with open(file_path, "r") as f:
            data_dict = json.load(f)

        pg_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        transformed_data = []
        for row in data_dict:
            cursor.execute(f"""
                                  SELECT province_code FROM public.ref_province_mapping WHERE province_name = %s
                                  """, (row['province'],))
            result = cursor.fetchone()
            row['province_id'] = result[0] if result else None
            transformed_data.append(row)
        
        df = pd.DataFrame(transformed_data, columns=SOURCE_COLUMNS_2).astype(str)

        df.to_json(FILE_PATH_2, orient="records", default_handler=str)

        kwargs['ti'].xcom_push(key="file_path_2", value=FILE_PATH_2)

        log_status(process_name, random_value, "success")
        return transformed_data
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

def load_to_postgres(**kwargs):
    """Load extracted data into Postgres."""
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_clickhouse", key="random_value")
    process_name = "load_postgres"
    log_status(process_name, random_value, "pending")

    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids="transform_data", key="file_path_2")
    
        if not file_path:
            raise ValueError("No file path received from Transform Data.")
    
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
        INSERT INTO public.mart_tms_visitreport_province (visitreport, email, user_name, unit_mtel, direktorat_mtel, site_id, 
                                                        site_name, comment_tower, submit_date, site_visit_requet, region, province, province_id, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now() + INTERVAL '7 hours')
        ON CONFLICT (visitreport, email, site_visit_requet) 
        DO UPDATE SET 
            user_name = EXCLUDED.user_name,
            unit_mtel = EXCLUDED.unit_mtel,
            direktorat_mtel = EXCLUDED.direktorat_mtel,
            site_id = EXCLUDED.site_id,
            site_name = EXCLUDED.site_name,
            comment_tower = EXCLUDED.comment_tower,
            submit_date = EXCLUDED.submit_date,
            region = EXCLUDED.region,
            province = EXCLUDED.province,
            province_id = EXCLUDED.province_id,
            updated_at = now() + INTERVAL '7 hours';
        """
        
        for __, row in df.iterrows():
            cursor.execute(insert_query, (row['visitreport'], row['email'], row['user_name'], row['unit_mtel'], row['direktorat_mtel'], 
                                    row['direktorat_mtel'], row['site_name'], row['comment_tower'], row['submit_date'], row['site_visit_requet'], 
                                    row['region'], row['province'], row['province_id']))
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

transform_data_first = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

load_postgres = PythonOperator(
    task_id="load_postgres",
    python_callable=load_to_postgres,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

extract_clickhouse >> transform_data_first >> load_postgres
