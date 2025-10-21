import json
import pandas as pd
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

MYSQL_CONN_ID = "db_oneflux_tms"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
MYSQL_DATABASE = "db_tms"
MYSQL_TABLE = "tabTask Rep"
CLICKHOUSE_DATABASE = "spmk"
CLICKHOUSE_TABLE = "tabtaskrep"
FILE_PATH = "/tmp/debug_DL_spmk_tabtaskrep.json"
DAG_ID = "DL_spmk_tabtaskrep"
DAG_INTERVAL = "20 17 * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Lake" 
TAGS = ["dl", "spmk", "tabtaskrep"]

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
    
def extract_from_mysql(**kwargs):
    """Extract data from MySQL and store it as a DataFrame."""
    random_value = random.randint(1000, 9999)  # Angka acak 4 digit
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format YYYYMMDDHHMMSS
    RANDOM_VALUE = f"{random_value}_{timestamp}"
    process_name = "extract_mysql"
    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql = f"SELECT * FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`"
        engine = mysql_hook.get_sqlalchemy_engine()
        chunks = []

        with engine.connect() as connection:
            result = connection.execution_options(stream_results=True).execute(sql)
            while True:
                rows = result.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                df_chunk = pd.DataFrame(rows, columns=result.keys())
                chunks.append(df_chunk)

        df = pd.concat(chunks, ignore_index=True).astype(str)
        df.to_json(FILE_PATH, orient="records", default_handler=str)

        kwargs['ti'].xcom_push(key="file_path", value=FILE_PATH)
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise

def load_to_clickhouse(**kwargs):
    """Load extracted data into ClickHouse."""
    random_value = kwargs['ti'].xcom_pull(task_ids="extract_mysql", key="random_value")
    process_name = "load_clickhouse"
    log_status(process_name, random_value, "pending")

    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids="extract_mysql", key="file_path")
    
        if not file_path:
            raise ValueError("No file path received from MySQL extraction.")
    
        with open(file_path, "r") as f:
            data_dict = json.load(f)

        df = pd.DataFrame(data_dict, dtype=str)  
        df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
        df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)

        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        hook.execute(f"TRUNCATE TABLE `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}`")

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

extract_mysql = PythonOperator(
    task_id="extract_mysql",
    python_callable=extract_from_mysql,
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

extract_mysql >> load_clickhouse