import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

MYSQL_CONN_ID = "db_oneflux_tms"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
MYSQL_DATABASE = "db_tms"
MYSQL_TABLE = "tabSite Visit Request"
CLICKHOUSE_DATABASE = "tms"
CLICKHOUSE_TABLE = "tabsitevisitrequest"
FILE_PATH = "/tmp/batch_"
DAG_ID = "DL_tms_tabsitevisitrequest"
DAG_INTERVAL = "*/5 * * * *"
CHUNK_SIZE = 500
BATCH_SIZE = 1500
BATCH_NO = 9

LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "delta"
LOG_KATEGORI = "Data Lake" 
TAGS = ["dl", "tms", "tabsitevisitrequest"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1, 19, 5),
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
    dag_name = f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"
    
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

def get_latest_modified(**kwargs):
    process_name = "get_latest_modified"
    random_value = random.randint(1000, 9999)  # Angka acak 4 digit
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format YYYYMMDDHHMMSS
    RANDOM_VALUE = f"{random_value}_{timestamp}"
    log_status(process_name, RANDOM_VALUE, "pending")
    
    try:
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        sql = f"SELECT MAX(modified) FROM `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}`"
        result = hook.execute(sql)
        latest_modified = result[0][0] if result and result[0] and result[0][0] else "1900-01-01 00:00:00"
        ti = kwargs['ti']
        ti.xcom_push(key='latest_modified', value=latest_modified)
        del hook
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def count_total_rows(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = "count_total_rows"
    log_status(process_name, random_value, "pending")

    try:
        ti = kwargs['ti']
        latest_modified = ti.xcom_pull(task_ids='get_latest_modified', key='latest_modified')
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql = f"SELECT COUNT(*) FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}` WHERE modified > '{latest_modified}'"
        total_rows = mysql_hook.get_first(sql)[0]
        ti.xcom_push(key='total_rows', value=total_rows)
        del mysql_hook
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise


def extract_batch(batch_number, **kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = f"extract_batch_{batch_number}"
    log_status(process_name, random_value, "pending")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, connect_args={"connect_timeout": 600000, "autocommit": True})
    engine = mysql_hook.get_sqlalchemy_engine()
    try:
        ti = kwargs['ti']
        total_rows = ti.xcom_pull(task_ids='count_rows', key='total_rows')
        latest_modified = ti.xcom_pull(task_ids='get_latest_modified', key='latest_modified')
        offset = batch_number * BATCH_SIZE
        
        if offset >= total_rows:
            # Tetap buat file JSON kosong jika batch ini tidak diperlukan
            empty_path = f"{FILE_PATH}{batch_number}.json"
            with open(empty_path, "w") as f:
                json.dump([], f)  # Menyimpan daftar kosong sebagai JSON
            ti.xcom_push(key=f"file_path_{batch_number}", value=empty_path)
            log_status(process_name, random_value, "success")
            return  
        
        sql = f"""
            SELECT * FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`
            WHERE modified > '{latest_modified}'
            ORDER BY modified ASC
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """
        

        chunks = []
        chunk_size = CHUNK_SIZE  # Ambil 5000 baris per batch
        with engine.connect() as connection:
            result = connection.execution_options(stream_results=True).execute(sql)
            
            while True:
                rows = result.fetchmany(chunk_size)  # Ambil batch
                if not rows:
                    break  # Keluar jika tidak ada data lagi
                
                df_chunk = pd.DataFrame(rows, columns=result.keys())  # Konversi ke DataFrame
                chunks.append(df_chunk)  # Simpan batch

        if chunks:
            df = pd.concat(chunks, ignore_index=True).astype(str)
        else:
            df = pd.DataFrame()  # DataFrame kosong jika tidak ada data

        df.to_json(f"{FILE_PATH}{batch_number}.json", orient="records", default_handler=str)
        ti.xcom_push(key=f"file_path_{batch_number}", value=f"{FILE_PATH}{batch_number}.json")
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise RuntimeError(f"Error during SQL execution: {e}")
    finally:
        engine.dispose()  # Pastikan koneksi ditutup setelah digunakan

    

def load_to_clickhouse(**kwargs):  
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = "load_clickhouse"
    log_status(process_name, random_value, "pending")

    try:  
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        
        for batch_number in range(BATCH_NO):
            file_path = f"{FILE_PATH}{batch_number}.json"
            
            if not file_path:
                log_status(process_name, random_value, "failed", str(e))
                raise ValueError("No file path received from MySQL extraction.")
            
            with open(file_path, "r") as f:
                data_dict = json.load(f)

            if not data_dict:
                continue

            # Convert to DataFrame with explicit dtype
            df = pd.DataFrame(data_dict, dtype=str)  

            df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
            df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)
            
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)

            insert_query = f"INSERT INTO `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}` VALUES"
            values = ', '.join(
                f"""({', '.join(f"'{x}'" for x in row)})""" 
                for row in df.itertuples(index=False, name=None)
            )
            
            hook.execute(insert_query + values)
        log_status(process_name, random_value, "success")
    except Exception as e:
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
                retries=15,
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
    provide_context=True,
    dag=dag
)

get_latest_modified_task >> count_rows_task >> extraction_tasks >> load_clickhouse_task
