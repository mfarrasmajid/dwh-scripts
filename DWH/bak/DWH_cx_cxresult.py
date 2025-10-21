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
from pytz import timezone

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- DAG Configuration ---
# Source: ClickHouse Data Warehouse
SOURCE_CONN_ID = "clickhouse_mitratel"
SOURCE_DATABASE = "cx"
SOURCE_TABLE = "tabcxresult"
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_cx"
TARGET_TABLE = "fact_cxresult"

# DAG parameters
FILE_PATH = "/tmp/batch_DWH_factcxresult_"
DAG_ID = "DWH_cx_cxresult"
DAG_INTERVAL = "30 17 * * *"
CHUNK_SIZE = 5000
BATCH_SIZE = 10000
BATCH_NO = 9
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dwh", "cx", "factcxresult"]

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

def get_latest_modified(**kwargs):
    process_name = "get_latest_modified"
    random_value = random.randint(1000, 9999)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    RANDOM_VALUE = f"{random_value}_{timestamp}"

    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        sql = f"SELECT MAX(updated_at) FROM {TARGET_DATABASE}.{TARGET_TABLE}"
        # Execute the query directly on the hook, which handles the connection lifecycle.
        result = hook.get_conn().execute(sql)
        latest_modified = result[0][0] if result and result[0] and len(result[0]) > 0 else "1900-01-01 00:00:00"

        ti = kwargs['ti']
        ti.xcom_push(key='latest_modified', value=str(latest_modified))
        ti.xcom_push(key='random_value', value=RANDOM_VALUE)
        log.info(f"Latest modified date fetched: {latest_modified}")
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log.error(f"Error fetching latest_modified from ClickHouse: {e}")
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise RuntimeError(f"Error fetching latest_modified from ClickHouse: {e}")
    

def count_total_rows(**kwargs):
    process_name = "count_total_rows"
    ti = kwargs['ti']
    random_value = ti.xcom_pull(task_ids="get_latest_modified", key="random_value")
    log_status(process_name, random_value, "pending")

    try:
        hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
        sql = f"""
            SELECT COUNT(*) FROM {SOURCE_DATABASE}.{SOURCE_TABLE}
        """
        # Execute the query directly on the hook, which handles the connection lifecycle.
        result = hook.get_conn().execute(sql)
        total_rows = result[0][0] if result and result[0] and len(result[0]) > 0 else 0

        ti.xcom_push(key='total_rows', value=total_rows)
        log.info(f"Total rows to process: {total_rows}")
        log_status(process_name, random_value, "success")
    except Exception as e:
        log.error(f"Error counting rows from ClickHouse: {e}")
        log_status(process_name, random_value, "failed", str(e))
        raise RuntimeError(f"Error counting rows from ClickHouse: {e}")

def extract_batch(batch_number, **kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = f"extract_batch_{batch_number}"
    log_status(process_name, random_value, "pending")

    hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
    try:
        ti = kwargs['ti']
        total_rows = ti.xcom_pull(task_ids='count_rows', key='total_rows')
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
                t.name,
                t.owner,
                t.date,
                t.type_journey,
                t.value,
                t.description,
                trimBoth(cat) AS kategori_parsed,
                t.value_detail,
                t.name_or_id,
                t.category,
                t.reference_doctype
            FROM {SOURCE_DATABASE}.{SOURCE_TABLE} AS t
            ARRAY JOIN splitByRegexp('\\s*,\\s*', ifNull(t.category, '')) AS cat
            WHERE trim(cat) <> ''
              AND toDate(t.date) >= toDate('2025-06-20')
            ORDER BY t.name ASC
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


def load_to_clickhouse(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = "load_clickhouse"
    log_status(process_name, random_value, "pending")

    try:
        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        
        # --- START FIX: Truncate the table before loading ---
        client = hook.get_conn()
        truncate_query = f"TRUNCATE TABLE {TARGET_DATABASE}.{TARGET_TABLE}"
        client.execute(truncate_query)
        log.info(f"Successfully truncated table {TARGET_DATABASE}.{TARGET_TABLE}")
        # --- END FIX ---
        
        target_columns = [
            'name', 'owner', 'date', 'type_journey', 'value', 'description', 'kategori_parsed',
            'value_detail', 'name_or_id', 'category', 'reference_doctype'
        ]

        conn_params = hook.get_connection(TARGET_CONN_ID)
        client = ClickHouseClient(host=conn_params.host, 
                                 port=conn_params.port, 
                                 user=conn_params.login, 
                                 password=conn_params.password)
        
        for batch_number in range(BATCH_NO):
            file_path = kwargs['ti'].xcom_pull(task_ids=f"extraction_tasks.extract_batch_{batch_number}", key=f"file_path_{batch_number}")
            
            if not file_path:
                log.warning(f"No file path found for batch {batch_number}. Skipping.")
                continue

            with open(file_path, "r") as f:
                data_dict = json.load(f)

            if not data_dict:
                log.info(f"File for batch {batch_number} is empty. Skipping insertion.")
                continue

            df = pd.DataFrame(data_dict, dtype=str)
            
            log.info(f"Inserting {len(df)} rows from batch {batch_number} into ClickHouse...")
            
            rows_to_insert = [tuple(row) for row in df.itertuples(index=False)]
            
            insert_query = f"INSERT INTO {TARGET_DATABASE}.{TARGET_TABLE} ({', '.join(target_columns)}) VALUES"
            
            client.execute(insert_query, rows_to_insert)

        log.info(f"Successfully loaded all batches to {TARGET_DATABASE}.{TARGET_TABLE}")
        log_status(process_name, random_value, "success")
    except Exception as e:
        log.error(f"Error during ClickHouse load: {e}")
        log_status(process_name, random_value, "failed", str(e))
        raise

def create_extraction_tasks(dag):
    with TaskGroup("extraction_tasks", dag=dag) as extraction_tasks:
        for batch_number in range(BATCH_NO):
            PythonOperator(
                task_id=f"extract_batch_{batch_number}",
                python_callable=extract_batch,
                op_kwargs={'batch_number': batch_number},
                provide_context=True,
                retries=3,
                retry_delay=timedelta(seconds=30),
                dag=dag,
            )
    return extraction_tasks

get_latest_modified_task = PythonOperator(
    task_id="get_latest_modified",
    python_callable=get_latest_modified,
    provide_context=True,
    dag=dag
)

count_rows_task = PythonOperator(
    task_id="count_rows",
    python_callable=count_total_rows,
    provide_context=True,
    dag=dag
)

extraction_tasks = create_extraction_tasks(dag)

load_clickhouse_task = PythonOperator(
    task_id="load_clickhouse",
    python_callable=load_to_clickhouse,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

get_latest_modified_task >> count_rows_task >> extraction_tasks >> load_clickhouse_task
