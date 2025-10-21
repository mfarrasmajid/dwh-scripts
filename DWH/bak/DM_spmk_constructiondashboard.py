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
SOURCE_DATABASE = "dwh_spmk"
SOURCE_TABLE = "fact_spmk"
TARGET_CONN_ID = "db_datamart_mitratel"
TARGET_DATABASE = "datamart"
TARGET_TABLE = "mart_spmk_constructiondashboard"
FILE_PATH = "/tmp/debug_DM_spmk_constructiondashboard.json"
DAG_ID = "DM_spmk_constructiondashboard"
DAG_INTERVAL = "30 17 * * *"
CHUNK_SIZE = 5000
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "truncate"
LOG_KATEGORI = "Data Mart"
TAGS = ["dm", "spmk", "constructiondashboard"]

EXTRACT_QUERY = """
SELECT wo_number, project_id, area, region, province,
    spmk_number, is_spmk_cancel, spmk_status, task_number, task_desc,
    task_type, task_status, bast_document, boq, boq_status,
    mitra, scope_of_work, bast_name, nomor_bast, bast_status,
    pekerjaan, po_number, po_status, gr_sap_status
FROM dwh_spmk.fact_spmk
"""

SOURCE_COLUMNS=['wo_number', 'project_id', 'area', 'region', 'province',
    'spmk_number', 'is_spmk_cancel', 'spmk_status', 'task_number', 'task_desc',
    'task_type', 'task_status', 'bast_document', 'boq', 'boq_status',
    'mitra', 'scope_of_work', 'bast_name', 'nomor_bast', 'bast_status',
    'pekerjaan', 'po_number', 'po_status', 'gr_sap_status',
    'spmk', 'boq_value', 'bast', 'po', 'gr']

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
            row[0] = row[0] if row[0] is not None else "None" # wo_number
            row[1] = row[1] if row[1] is not None else "None" # project_id
            row[5] = row[5] if row[5] is not None else "None" # spmk_number
            row[8] = row[8] if row[8] is not None else "None" # task_number
            row[12] = row[12] if row[12] is not None else "None" # bast_document
            row[13] = row[13] if row[13] is not None else "None" # boq_number
            row[17] = row[17] if row[17] is not None else "None" # bast_name
            row[18] = row[18] if row[18] is not None else "None" # nomor_bast
            row[21] = row[21] if row[21] is not None else "None" # po_number
            spmk_number = row[5]  # Adjust based on index of `spmk_number`
            is_spmk_cancel = row[6]  # Adjust based on index of `is_spmk_cancel`
            spmk_status = row[7]  # Adjust based on index of `spmk_status`
            boq = row[13]  # Adjust based on index of `boq`
            boq_status = row[14]  # Adjust based on index of `boq_status`
            bast_document = row[12]  # Adjust based on index of `bast_document`
            nomor_bast = row[18]  # Adjust based on index of `nomor_bast`
            bast_status = row[19]  # Adjust based on index of `bast_status`
            po_number = row[21]  # Adjust based on index of `po_number`
            po_status = row[22]  # Adjust based on index of `po_status`
            gr_sap_status = row[23]  # Adjust based on index of `gr_sap_status`

            # Add new computed columns
            spmk = "SPMK" if spmk_number and is_spmk_cancel == "0" and spmk_status == "Approved" else "NO SPMK"
            
            boq_value = "BOQ On Process"
            if boq:
                if boq_status == "Approved":
                    boq_value = "BOQ Done"
                elif boq_status == "Cancelled":
                    boq_value = "BOQ Cancelled"
                elif boq_status == "Returned":
                    boq_value = "BOQ Returned"
                elif boq_status == "Draft":
                    boq_value = "BOQ Draft"
            
            bast = "BAST Done" if (bast_document != "None" or nomor_bast != "None") and bast_status == "Approved" else \
                "BAST Returned" if (bast_document != "None" or nomor_bast != "None") and bast_status == "Returned" else \
                "BAST Cancelled" if (bast_document != "None" or nomor_bast != "None") and bast_status == "Cancelled" else \
                "Not Yet BAST" if bast_document == "None" and nomor_bast == "None" and bast_status == "" else "BAST On Process"
            
            po = "PO" if po_number and po_status == "Approved" else "Not Yet PO"

            gr = "GR Done" if gr_sap_status == "Success" else "GR Failed" if gr_sap_status == "Failed" else "Not Yet GR"
            row.append(spmk)
            row.append(boq_value)
            row.append(bast)
            row.append(po)
            row.append(gr)

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
        # df = df.replace({'': None, 'None': None})

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO public.mart_spmk_constructiondashboard (
            wo_number, project_id, area, region, province, spmk_number, is_spmk_cancel, spmk_status,
            task_number, task_desc, task_type, task_status, bast_document, boq_number, boq_status, mitra,
            scope_of_work, bast_name, nomor_bast, bast_status, pekerjaan, po_number, po_status,
            gr_sap_status, spmk, boq, bast, po, gr
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (wo_number, project_id, spmk_number, task_number, bast_name, bast_document, boq_number, nomor_bast, po_number) DO UPDATE 
        SET updated_at = NOW(), 
            spmk = EXCLUDED.spmk,
            boq = EXCLUDED.boq,
            bast = EXCLUDED.bast,
            po = EXCLUDED.po,
            gr = EXCLUDED.gr
        """

        for __, row in df.iterrows():
            cursor.execute(insert_query, (row['wo_number'], row['project_id'], row['area'], row['region'], row['province'],
                                            row['spmk_number'], row['is_spmk_cancel'], row['spmk_status'], row['task_number'], row['task_desc'],
                                            row['task_type'], row['task_status'], row['bast_document'], row['boq'], row['boq_status'],
                                            row['mitra'], row['scope_of_work'], row['bast_name'], row['nomor_bast'], row['bast_status'],
                                            row['pekerjaan'], row['po_number'], row['po_status'], row['gr_sap_status'],
                                            row['spmk'], row['boq_value'], row['bast'], row['po'], row['gr']))
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
