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

SOURCE_CONN_ID = "clickhouse_dwh_mitratel"
SOURCE_DATABASE = "dwh_om"
SOURCE_TABLE = "fact_troubleticket"
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "datamart"
TARGET_TABLE = "mart_om_troubleticketcomp"
FILE_PATH = "/tmp/batch_DM_tabtroubleticketcomp_"
DAG_ID = "DM_om_troubleticket"
DAG_INTERVAL = "4-59/5 * * * *"
CHUNK_SIZE = 5000
BATCH_SIZE = 10000
BATCH_NO = 9
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dm", "om", "troubleticketcomp"]

COLUMNS = [
    "name", "creation", "modified", "modified_by", "owner", "docstatus", "parent", "parentfield",
    "parenttype", "idx", "status", "tt_ant_id", "mitra_om", "engineer", "customer", "tower_id",
    "pid", "priority", "disctrict_city", "region", "tt_description", "ts_open", "ts_need_assign",
    "ts_on_progress", "ts_pickup", "ts_departure", "ts_arrived", "ts_resolved", "ts_closed",
    "pic", "detail_issue_type", "issue_type", "reference", "tower_name", "tower_owner",
    "actual_category", "closed_by", "sla_status", "mttr_hours",
    "aging",
    "portofolio",
    "area",
    "address",
    "tenant_id",
    "sid_tenant",
    "tanggal_request_ant",
    "service_level_agreement",
    "agreement_status",
    "tt_id_ampuhc",
    "class_of_service",
    "maintenance_zone",
    "mttr" ]

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

def get_latest_modified(**kwargs):
    process_name = "get_latest_modified"
    random_value = random.randint(1000, 9999)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    RANDOM_VALUE = f"{random_value}_{timestamp}"

    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        sql = f'SELECT MAX(modified) FROM public."{TARGET_TABLE}"'
        latest_modified = hook.get_first(sql)[0] or "1900-01-01 00:00:00"

        ti = kwargs['ti']
        ti.xcom_push(key='latest_modified', value=str(latest_modified))  # to make it XCom-safe
        ti.xcom_push(key='random_value', value=RANDOM_VALUE)

        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise
    

def count_total_rows(**kwargs):
    process_name = "count_total_rows"
    ti = kwargs['ti']
    random_value = ti.xcom_pull(task_ids="get_latest_modified", key="random_value")
    log_status(process_name, random_value, "pending")

    try:
        latest_modified = ti.xcom_pull(task_ids='get_latest_modified', key='latest_modified') or "1900-01-01 00:00:00"

        hook = ClickHouseHook(clickhouse_conn_id=SOURCE_CONN_ID)
        sql = f"""
            SELECT COUNT(*) FROM {SOURCE_DATABASE}.{SOURCE_TABLE}
            WHERE modified > '{latest_modified}'
        """
        result = hook.get_conn().execute(sql)
        total_rows = result[0][0]  # Fetch count from first row, first column

        ti.xcom_push(key='total_rows', value=total_rows)
        log_status(process_name, random_value, "success")
    except Exception as e:
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
        latest_modified = ti.xcom_pull(task_ids='get_latest_modified', key='latest_modified') or "1900-01-01 00:00:00"
        offset = batch_number * BATCH_SIZE

        if offset >= total_rows:
            empty_path = f"{FILE_PATH}{batch_number}.json"
            with open(empty_path, "w") as f:
                json.dump([], f)
            ti.xcom_push(key=f"file_path_{batch_number}", value=empty_path)
            log_status(process_name, random_value, "success")
            return

        sql = f"""
            SELECT * FROM {SOURCE_DATABASE}.{SOURCE_TABLE}
            WHERE modified >= '{latest_modified}'
            ORDER BY modified ASC
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """

        conn = hook.get_conn()
        rows = conn.execute(sql)
        columns = [col[0] for col in conn.execute(f"DESCRIBE TABLE {SOURCE_DATABASE}.{SOURCE_TABLE}")]

        if rows:
            df = pd.DataFrame(rows, columns=columns).astype(str)
        else:
            df = pd.DataFrame()

        output_path = f"{FILE_PATH}{batch_number}.json"
        df.to_json(output_path, orient="records", default_handler=str)
        ti.xcom_push(key=f"file_path_{batch_number}", value=output_path)
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise RuntimeError(f"Error during ClickHouse SQL execution: {e}")

def convert_timedelta(value):
    if isinstance(value, timedelta):
        # Convert timedelta to "HH:MM:SS" format
        total_seconds = int(value.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    elif isinstance(value, str) and value.startswith("0 days"):
        try:
            parts = value.split(" ")[-1].split(":")
            return f"{int(parts[0]):02}:{int(parts[1]):02}:{int(float(parts[2])):02}"
        except:
            return None
    return value

def load_to_postgres(**kwargs):  
    process_name = "load_postgres"
    ti = kwargs['ti']
    random_value = ti.xcom_pull(task_ids="get_latest_modified", key="random_value")
    log_status(process_name, random_value, "pending")

    try:  
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        for batch_number in range(BATCH_NO):
            file_path = f"{FILE_PATH}{batch_number}.json"

            with open(file_path, "r") as f:
                data_dict = json.load(f)

            if not data_dict:
                continue  # Skip empty batch

            df = pd.DataFrame(data_dict, dtype=str)
            if df.empty:
                log_status(process_name, random_value, "success (no data)")
                continue

            df = df[COLUMNS]
            df.applymap(convert_timedelta)
            df = df.applymap(lambda x: x.replace("\\", "") if isinstance(x, str) else x)
            df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)

            # 👇 Fix: clean invalid timestamp input
            df = df.replace({'None': None, '': None, 'NaT': None})

            # Prepare dynamic column list and values
            columns = list(df.columns)

            # Index of conflict column (assumed to be 'name')
            name_index = columns.index("name")

            # Deduplicate rows based on conflict key
            seen_names = set()
            unique_values = []
            for row in df.values.tolist():
                name = row[name_index]
                if name not in seen_names:
                    seen_names.add(name)
                    unique_values.append(row)

            values = unique_values

            column_str = ', '.join(f'"{col}"' for col in columns)

            insert_query = f"""
            INSERT INTO public."{TARGET_TABLE}" ({column_str})
            VALUES %s
            ON CONFLICT (name) DO UPDATE SET
            {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in columns if col != "name")}
            """

            execute_values(cursor, insert_query, values)

        conn.commit()
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise RuntimeError(f"Error loading to Postgres: {e}")

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

load_postgres = PythonOperator(
    task_id="load_postgres",
    python_callable=load_to_postgres,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

get_latest_modified_task >> count_rows_task >> extraction_tasks >> load_postgres
