import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from psycopg2.extras import execute_values
from airflow.utils.task_group import TaskGroup
import logging
from clickhouse_driver import Client as ClickHouseClient

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- DAG Configuration ---
# Source: ClickHouse Data Warehouse
SOURCE_CONN_ID = "clickhouse_dwh_mitratel"
SOURCE_DATABASE = "dwh_cx"
SOURCE_TABLE = "fact_cxresult"

# Target: PostgreSQL Data Mart
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "public"
TARGET_TABLE = "mart_cx_cxresult"

# DAG parameters
FILE_PATH = "/tmp/batch_DWH_martcxresult_"
DAG_ID = "DM_cx_cxresult"  # Renamed DAG_ID
DAG_INTERVAL = "45 17 * * *"
BATCH_SIZE = 10000
BATCH_NO = 9  # Number of parallel batches to process
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dm", "postgres", "etl"]

# --- Default Airflow Arguments ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=DAG_INTERVAL,
    default_args=default_args,
    catchup=False, tags=TAGS
)

# --- Logging Function ---
def log_status(process_name, mark, status, error_message=None):
    """Insert or update the log table."""
    pg_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dag_name = f"{TARGET_DATABASE}.{TARGET_TABLE}"
    
    try:
        if status == "pending":
            log.info(f"Logging process '{process_name}' with status 'pending'...")
            cursor.execute(
                f"""
                INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
            )
        else:
            log.info(f"Updating process '{process_name}' with status '{status}'...")
            cursor.execute(
                f"""
                UPDATE {LOG_TABLE} SET status = %s, end_time = %s, error_message = %s
                WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
                """, (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
            )
        
        conn.commit()
        log.info(f"Log status updated successfully for '{process_name}'")
    except Exception as e:
        log.error(f"Error logging status for process '{process_name}': {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# --- ETL Functions ---
def get_total_rows(**kwargs):
    """Counts the total rows to extract from the ClickHouse DWH table."""
    process_name = "get_total_rows"
    random_value = random.randint(1000, 9999)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    RANDOM_VALUE = f"{random_value}_{timestamp}"

    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
        sql = f"SELECT COUNT(*) FROM {SOURCE_DATABASE}.{SOURCE_TABLE}"
        result = hook.get_conn().execute(sql)
        total_rows = result[0][0] if result and result[0] and len(result[0]) > 0 else 0

        ti = kwargs['ti']
        ti.xcom_push(key='total_rows', value=total_rows)
        ti.xcom_push(key='random_value', value=RANDOM_VALUE)
        log.info(f"Total rows to process: {total_rows}")
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log.error(f"Error counting rows from ClickHouse: {e}")
        log_status(process_name, RANDOM_value, "failed", str(e))
        raise RuntimeError(f"Error counting rows from ClickHouse: {e}")

def extract_from_clickhouse(batch_number, **kwargs):
    """
    Extracts data in batches from ClickHouse and saves to a JSON file.
    """
    random_value = kwargs['ti'].xcom_pull(task_ids="get_total_rows", key="random_value")
    process_name = f"extract_batch_{batch_number}"
    log_status(process_name, random_value, "pending")

    hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
    try:
        ti = kwargs['ti']
        total_rows = ti.xcom_pull(task_ids='get_total_rows', key='total_rows')
        offset = batch_number * BATCH_SIZE

        if offset >= total_rows:
            log.info(f"Batch {batch_number} is out of bounds (offset {offset} >= total rows {total_rows}). Skipping extraction.")
            empty_path = f"{FILE_PATH}{batch_number}.json"
            with open(empty_path, "w") as f:
                json.dump([], f)
            ti.xcom_push(key=f"file_path_{batch_number}", value=empty_path)
            log_status(process_name, random_value, "success")
            return

        columns_to_select = [
            'name', 'owner', 'date', 'type_journey', 'value', 'description', 'kategori_parsed',
            'value_detail', 'name_or_id', 'category', 'reference_doctype'
        ]

        sql = f"""
            SELECT
                name, owner, date, type_journey, value, description, kategori_parsed,
                value_detail, name_or_id, category, reference_doctype
            FROM {SOURCE_DATABASE}.{SOURCE_TABLE}
            ORDER BY name ASC
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """
        
        log.info(f"Executing SQL query for batch {batch_number}: {sql}")
        rows = hook.get_conn().execute(sql)

        if rows:
            df = pd.DataFrame(rows, columns=columns_to_select)
        else:
            df = pd.DataFrame()

        output_path = f"{FILE_PATH}{batch_number}.json"
        df.to_json(output_path, orient="records", default_handler=str)
        ti.xcom_push(key=f"file_path_{batch_number}", value=output_path)
        log.info(f"Successfully extracted {len(df)} rows for batch {batch_number} to {output_path}")
        log_status(process_name, random_value, "success")
    except Exception as e:
        log.error(f"Error during ClickHouse SQL execution for batch {batch_number}: {e}")
        log_status(process_name, random_value, "failed", str(e))
        raise RuntimeError(f"Error during ClickHouse SQL execution for batch {batch_number}: {e}")


def load_to_postgres(**kwargs):
    """
    Loads data from the extracted JSON files into the PostgreSQL data mart.
    """
    random_value = kwargs['ti'].xcom_pull(task_ids="get_total_rows", key="random_value")
    process_name = "load_to_postgres"
    log_status(process_name, random_value, "pending")

    pg_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    try:
        # Truncate the table before loading to ensure a fresh dataset
        pg_hook.run(f"TRUNCATE TABLE {TARGET_TABLE} CASCADE;")
        log.info(f"Truncated target table: {TARGET_TABLE}")
        
        target_columns = [
            'name', 'owner', 'date', 'type_journey', 'value', 'description', 'kategori_parsed',
            'value_detail', 'name_or_id', 'category', 'reference_doctype'
        ]
        
        for batch_number in range(BATCH_NO):
            file_path = kwargs['ti'].xcom_pull(task_ids=f"extraction_tasks.extract_from_clickhouse_batch_{batch_number}", key=f"file_path_{batch_number}")
            
            if not file_path:
                log.warning(f"No file path found for batch {batch_number}. Skipping.")
                continue

            with open(file_path, "r") as f:
                data_dict = json.load(f)

            if not data_dict:
                log.info(f"File for batch {batch_number} is empty. Skipping insertion.")
                continue

            df = pd.DataFrame(data_dict, dtype=str)
            
            log.info(f"Inserting {len(df)} rows from batch {batch_number} into PostgreSQL...")
            
            # --- FIX: Reverted to using execute_values for broader compatibility ---
            rows_to_insert = [tuple(x) for x in df.to_numpy()]
            insert_query = f"INSERT INTO {TARGET_TABLE} ({', '.join(target_columns)}) VALUES %s"
            
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_query, rows_to_insert)
                    conn.commit()
            # --- END FIX ---

        log.info(f"Successfully loaded all batches to {TARGET_TABLE}")
        log_status(process_name, random_value, "success")
    except Exception as e:
        log.error(f"Error during PostgreSQL load: {e}")
        log_status(process_name, random_value, "failed", str(e))
        raise

# --- Task Creation & Dependencies ---
def create_extraction_tasks(dag):
    with TaskGroup("extraction_tasks", dag=dag) as extraction_tasks:
        for batch_number in range(BATCH_NO):
            PythonOperator(
                task_id=f"extract_from_clickhouse_batch_{batch_number}",
                python_callable=extract_from_clickhouse,
                op_kwargs={'batch_number': batch_number},
                provide_context=True,
                retries=3,
                retry_delay=timedelta(seconds=30),
                dag=dag,
            )
    return extraction_tasks

get_total_rows_task = PythonOperator(
    task_id="get_total_rows",
    python_callable=get_total_rows,
    provide_context=True,
    dag=dag
)

extraction_tasks = create_extraction_tasks(dag)

load_to_postgres_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

get_total_rows_task >> extraction_tasks >> load_to_postgres_task
