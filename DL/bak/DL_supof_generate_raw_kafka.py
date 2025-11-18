from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import uuid
import mysql.connector
from clickhouse_driver import Client


# ---------------------
# 1) Default args & DAG
# ---------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='DL_supof_generate_raw_kafka',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dl', 'support_oneflux', 'init'],
)


# ---------------------
# 2) Python callable
# ---------------------
def main():
    # --- MySQL Connection ---
    mysql_conn = mysql.connector.connect(
        host=Variable.get("mysql_dbase_host"),
        user=Variable.get("mysql_dbase_user"),
        password=Variable.get("mysql_dbase_pass"),
    )
    mysql_cursor = mysql_conn.cursor()

    # Ambil semua database kecuali sistem
    mysql_cursor.execute("SHOW DATABASES;")
    databases = [
        db[0] for db in mysql_cursor.fetchall()
        if db[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    ]

    print(f"Akan mereplikasi {len(databases)} database: {databases}")

    # --- ClickHouse Connection ---
    ch_client = Client(
        host=Variable.get("clickhouse_datalake_host"),
        user=Variable.get("clickhouse_datalake_user"),
        password=Variable.get("clickhouse_datalake_pass"),
    )

    # Pastikan database target ada
    ch_client.execute("CREATE DATABASE IF NOT EXISTS kafka_mass")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS kafka_connector")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS support_oneflux")

    group_name = f"ch_consumer_{uuid.uuid4().hex[:16]}"

    # Kafka Source Table
    ch_client.execute(f"""
    CREATE TABLE IF NOT EXISTS kafka_mass.kafka_raw
    (
        _raw String
    )
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '192.168.101.164:9092',
             kafka_topic_list = 'mysql-mass-server.all-dbs',
             kafka_group_name = '{group_name}',
             kafka_format = 'RawBLOB'
    """)

    # Target Table (ReplacingMergeTree)
    ch_client.execute("""
    CREATE TABLE IF NOT EXISTS support_oneflux.all_mysql_data
    (
        source_database String,
        source_table String,
        pk_value String,
        payload String,
        is_deleted UInt8 DEFAULT 0,
        _op String,
        _ts DateTime
    )
    ENGINE = ReplacingMergeTree(_ts)
    ORDER BY (source_database, source_table, pk_value)
    """)

    # Materialized View
    ch_client.execute("""
    CREATE MATERIALIZED VIEW kafka_connector.mv_all_mysql_data
    TO support_oneflux.all_mysql_data
    AS
    WITH
        JSONExtractRaw(_raw, 'after')  AS j_after,
        JSONExtractRaw(_raw, 'before') AS j_before,
        JSONExtractRaw(_raw, 'source') AS j_src,
        JSON_VALUE(JSONExtractRaw(_raw, 'op'), '$') AS op
    SELECT
        JSON_VALUE(j_src, '$.db')    AS source_database,
        JSON_VALUE(j_src, '$.table') AS source_table,
        if(
            op = 'd',
            coalesce(
                JSON_VALUE(j_before, '$.id'),
                arrayElement(JSONExtractKeysAndValues(j_before, 'String').1, 1)
            ),
            coalesce(
                JSON_VALUE(j_after, '$.id'),
                arrayElement(JSONExtractKeysAndValues(j_after, 'String').1, 1)
            )
        ) AS pk_value,

        if(op = 'd', j_before, j_after) AS payload,

        op = 'd' AS is_deleted,
        op       AS _op,
        now()    AS _ts
    FROM kafka_mass.kafka_raw
    """)

    print("✅ Setup selesai! Semua database MySQL akan direplikasi real-time ke ClickHouse.")


# ---------------------
# 3) Task
# ---------------------
generate_task = PythonOperator(
    task_id='generate_raw_kafka',
    python_callable=main,
    dag=dag,
)
