import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

SOURCE_CONN_ID = "clickhouse_dwh_mitratel"
SOURCE_DATABASE = "dwh_spmk"
SOURCE_TABLE = "fact_const_2"
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "datamart"
TARGET_TABLE = "mart_spmk_const_v2"
FILE_PATH = "/tmp/debug_DM_spmk_const2.json"
DAG_ID = "DM_spmk_const2"
DAG_INTERVAL = "3-59/10 * * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dm", "spmk", "const2"]

EXTRACT_QUERY = """
SELECT * FROM dwh_spmk.fact_const_2
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

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=DAG_INTERVAL,
    default_args=default_args,
    catchup=False, tags=TAGS
)

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

def extract_from_clickhouse(**kwargs):
    random_value = random.randint(1000, 9999)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    RANDOM_VALUE = f"{random_value}_{timestamp}"
    process_name = "extract_clickhouse"
    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
        conn = clickhouse_hook.get_conn()
        data = conn.execute(EXTRACT_QUERY)
        df = pd.DataFrame(data, columns=SOURCE_COLUMNS).astype(str)
        df = df.where(pd.notnull(df), '')  # Replace None/NaN with empty string
        df.to_json(FILE_PATH, orient="records", default_handler=str)
        kwargs['ti'].xcom_push(key="file_path", value=FILE_PATH)
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def truncate_postgres(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_clickhouse", key="random_value")
    process_name = "truncate_postgres"
    log_status(process_name, random_value, "pending")
    try:
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE public.{TARGET_TABLE}")
        conn.commit()
        cursor.close()
        conn.close()
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

def load_to_postgres(**kwargs):
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
        df = df.where(pd.notnull(df), '')  # Replace None/NaN with empty string
        df = df.replace("NaT", None)       # Replace NaT with None for SQL NULL
        df = df.replace("None", None)       # Replace NaT with None for SQL NULL
        df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
        df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO public.{TARGET_TABLE} (
        project_id,
        spmk_number,
        spmk_cancel,
        spmk_value,
        spmk_status,
        spmk_sow,
        spmk_approved_date,
        spmk_create_date,
        boq_number,
        boq_status,
        regional,
        boq_sow,
        boq_standard_value,
        boq_addwork_value,
        boq_value,
        boq_create_date,
        boq_approved_date,
        boq_draft_date,
        boq_rejected_date,
        boq_pm_ro_date,
        boq_manager_codm_ro_date,
        boq_gm_area_date,
        boq_manager_construction_project_deployment_date,
        boq_gm_construction_project_management_date,
        boq_manager_procurement_operation_date,
        boq_vp_procurement_date,
        boq_manager_project_planning_date,
        pr_number,
        pr_item,
        pr_value,
        pr_create_date,
        po_number,
        po_item,
        po_create_date,
        po_value
        ) VALUES ({','.join(['%s']*len(SOURCE_COLUMNS))})
        """
        for __, row in df.iterrows():
            cursor.execute(insert_query, tuple(row[col] for col in SOURCE_COLUMNS))
        conn.commit()
        cursor.close()
        conn.close()
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

extract_clickhouse = PythonOperator(
    task_id="extract_clickhouse",
    python_callable=extract_from_clickhouse,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

truncate_postgres = PythonOperator(
    task_id="truncate_postgres",
    python_callable=truncate_postgres,
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

extract_clickhouse >> truncate_postgres >> load_postgres