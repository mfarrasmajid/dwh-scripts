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

SOURCE_CONN_ID = "clickhouse_mitratel"
SOURCE_DATABASE = "om"
SOURCE_TABLE = "tabtroubleticket"
TARGET_CONN_ID = "clickhouse_dwh_mitratel"
TARGET_DATABASE = "dwh_om"
TARGET_TABLE = "fact_troubleticket"
FILE_PATH = "/tmp/batch_DWH_tabtroubleticket_"
DAG_ID = "DWH_om_troubleticket"
DAG_INTERVAL = "3-59/5 * * * *"
CHUNK_SIZE = 5000
BATCH_SIZE = 10000
BATCH_NO = 9
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dwh", "om", "troubleticket"]

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
        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        sql = f"SELECT MAX(modified) FROM {TARGET_DATABASE}.{TARGET_TABLE}"
        result = hook.get_conn().execute(sql)

        # result: [(datetime or None,)]
        latest_modified = result[0][0] if result and result[0][0] else "1900-01-01 00:00:00"

        ti = kwargs['ti']
        ti.xcom_push(key='latest_modified', value=str(latest_modified))  # make XCom-safe
        ti.xcom_push(key='random_value', value=RANDOM_VALUE)

        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise RuntimeError(f"Error fetching latest_modified from ClickHouse: {e}")
    

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
            WHERE modified >= '{latest_modified}'
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
            WITH
                replaceRegexpAll(t.opening_time, '^[0-9]+\\s+days\\s+', '') AS opening_time_clean,

                parseDateTime64BestEffortOrNull(
                    concat(t.opening_date, ' ', opening_time_clean),
                    6
                ) AS opening_dt64
            SELECT
                t.*,
                CASE
                    WHEN t.priority IN ('ACCESS SITE','Besar','DAMAGE FOUND','RECOVERY GROUNDING STOLEN','ROOM TEMPERATURE','Major','Major MMP','Major Priority') THEN 'Major'
                    WHEN t.priority IN ('Kecil','Minor','Minor MMP','Minor Priority') THEN 'Minor'
                    WHEN t.priority IN ('Darurat','Emergency','Emergency MMP','Emergency Priority') THEN 'Emergency'
                    WHEN t.priority IN ('Critical','Critical MMP','Critical Priority','INPUT POWER LOSS','INPUT POWER LOSS (HUB SITE)') THEN 'Critical'
                    ELSE NULL
                END AS priority_mtel,
                CASE
                    WHEN trim(t.status) IN ('Open','On Progress','Need Assign')
                        AND toFloat64(mttr_hours) > 0
                    THEN
                        CASE
                            WHEN dateDiff('second', opening_dt64, now()) / 3600.0 < 0.7 * toFloat64(mttr_hours)
                            THEN 'In SLA'
                            WHEN dateDiff('second', opening_dt64, now()) / 3600.0 < toFloat64(mttr_hours)
                            THEN 'Akan Out SLA'
                            ELSE 'Out SLA'
                        END
                    ELSE NULL
                END AS detail_sla_status,
                CASE
                    WHEN nullIf(t.mo_reference,'') != 'None'
                    AND nullIf(t.case_finding_reference,'') != 'None'
                    AND coalesce(t.reference,'') != 'NMS' THEN 'External'
                    ELSE 'Internal'
                END AS tt_source
            FROM {SOURCE_DATABASE}.{SOURCE_TABLE} as t
            WHERE modified >= '{latest_modified}' AND toDate(parseDateTimeBestEffortOrNull(t.opening_date)) >= toDate('2025-01-01')
            ORDER BY modified ASC
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """

        conn = hook.get_conn()
        rows = conn.execute(sql)
        columns = [col[0] for col in conn.execute(f"DESCRIBE TABLE {SOURCE_DATABASE}.{SOURCE_TABLE}")]
        columns += ["priority_mtel", "detail_sla_status", "tt_source"]

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

def load_to_clickhouse(**kwargs):  
    random_value = kwargs['ti'].xcom_pull(task_ids="get_latest_modified", key="random_value")
    process_name = "load_clickhouse"
    log_status(process_name, random_value, "pending")

    try:  
        hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)
        
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
            
            hook = ClickHouseHook(clickhouse_conn_id=TARGET_CONN_ID)

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
    task_id="load_postgres",
    python_callable=load_to_clickhouse,
    do_xcom_push=False,
    provide_context=True,
    dag=dag,
)

get_latest_modified_task >> count_rows_task >> extraction_tasks >> load_clickhouse_task
